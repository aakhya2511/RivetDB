package replicatedrange

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
)

func TestMVCCReplicatedHistorySnapshotLeaderChangeAndRestart(t *testing.T) {
	cluster := newMVCCTestCluster(t, 3)
	cluster.elect(1)
	t1 := cluster.proposeMVCC(1, Command{Type: CommandPut, Key: []byte("foo"), Value: []byte("A")})
	t2 := cluster.proposeMVCC(1, Command{Type: CommandPut, Key: []byte("foo"), Value: []byte("B")})
	t3 := cluster.proposeMVCC(1, Command{Type: CommandDelete, Key: []byte("foo")})
	t4 := cluster.proposeMVCC(1, Command{Type: CommandPut, Key: []byte("foo"), Value: []byte("C")})
	if t1 >= t2 || t2 >= t3 || t3 >= t4 {
		t.Fatalf("timestamps not strict: %v", []mvcc.Timestamp{t1, t2, t3, t4})
	}
	assertGetAt(t, cluster.replicas[1], t1-1, "")
	assertGetAt(t, cluster.replicas[1], t1, "A")
	assertGetAt(t, cluster.replicas[1], t2, "B")
	assertGetAt(t, cluster.replicas[1], t3, "")
	assertGetAt(t, cluster.replicas[1], t4, "C")

	snapshot, err := cluster.replicas[1].SnapshotAt(t2)
	if err != nil {
		t.Fatal(err)
	}
	if flushErr := cluster.replicas[1].Flush(context.Background()); flushErr != nil {
		t.Fatal(flushErr)
	}
	for index := 0; index < 4; index++ {
		cluster.proposeMVCC(1, Command{Type: CommandPut, Key: []byte("foo"), Value: []byte{byte('D' + index)}})
		if flushErr := cluster.replicas[1].Flush(context.Background()); flushErr != nil {
			t.Fatal(flushErr)
		}
	}
	if compactErr := cluster.replicas[1].Compact(context.Background()); compactErr != nil {
		t.Fatal(compactErr)
	}
	reclaimed, err := cluster.replicas[1].ReclaimObsoleteTables(context.Background())
	if err != nil || reclaimed.Deleted == 0 {
		t.Fatalf("reclaim=%+v err=%v", reclaimed, err)
	}
	if value, getErr := snapshot.Get(context.Background(), []byte("foo")); getErr != nil || string(value) != "B" {
		t.Fatalf("snapshot=%q err=%v", value, getErr)
	}
	if oldest, ok := cluster.replicas[1].OldestSnapshotTimestamp(); !ok || oldest != t2 {
		t.Fatalf("oldest=%d/%v", oldest, ok)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.Get(context.Background(), []byte("foo")); !errors.Is(err, ErrSnapshotClosed) {
		t.Fatalf("closed=%v", err)
	}

	beforeElection := cluster.replicas[1].Status().MaxAppliedMVCC
	cluster.elect(2)
	if afterElection := cluster.replicas[2].Status().MaxAppliedMVCC; afterElection != beforeElection {
		t.Fatalf("leader-election no-op advanced MVCC watermark: before=%d after=%d", beforeElection, afterElection)
	}
	newTimestamp := cluster.proposeMVCC(2, Command{Type: CommandPut, Key: []byte("bar"), Value: []byte("leader2")})
	if newTimestamp <= t4 {
		t.Fatalf("leader timestamp regressed: %d <= %d", newTimestamp, t4)
	}
	for _, replica := range cluster.replicas {
		digest, digestErr := replica.DigestAt(context.Background(), t2)
		if digestErr != nil {
			t.Fatal(digestErr)
		}
		if replica != cluster.replicas[1] {
			expected, _ := cluster.replicas[1].DigestAt(context.Background(), t2)
			if digest != expected {
				t.Fatal("historical digest mismatch")
			}
		}
	}
	if _, err := cluster.replicas[3].GetAt(context.Background(), []byte("foo"), newTimestamp+1); !errors.Is(err, ErrReplicaBehind) {
		t.Fatalf("future read=%v", err)
	}
	assertGetAt(t, cluster.replicas[3], t1, "A")

	cluster.closeAll()
	cluster.openAll()
	cluster.elect(3)
	afterRestart := cluster.proposeMVCC(3, Command{Type: CommandPut, Key: []byte("baz"), Value: []byte("restart")})
	if afterRestart <= newTimestamp {
		t.Fatalf("restart timestamp regressed: %d <= %d", afterRestart, newTimestamp)
	}
	for _, replica := range cluster.replicas {
		assertGetAt(t, replica, t2, "B")
	}
}

func TestMVCCSubprocessCrashHistoricalRecovery(t *testing.T) {
	if os.Getenv("RIVETDB_MVCC_CRASH_CHILD") == "1" {
		root := os.Getenv("RIVETDB_MVCC_CRASH_ROOT")
		replica, err := Open(Options{RangeID: 1, NodeID: 1, ReplicaID: 1, Peers: []raft.NodeID{1}, Directory: root, MVCC: true,
			Clock: clock.NewMockAt(time.UnixMilli(1000)), ElectionTimeoutMin: 2, ElectionTimeoutMax: 2, HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(1, 2))})
		if err != nil {
			panic(err)
		}
		for replica.Status().Raft.Role != raft.Leader {
			if _, err = replica.Tick(); err != nil {
				panic(err)
			}
		}
		apply := func(command Command) mvcc.Timestamp {
			ts, index, _, waiter, applyErr := replica.ProposeMVCC(context.Background(), command)
			if applyErr != nil {
				panic(applyErr)
			}
			if applyErr = replica.Await(context.Background(), index, waiter); applyErr != nil {
				panic(applyErr)
			}
			return ts
		}
		t1 := apply(Command{Type: CommandPut, Key: []byte("foo"), Value: []byte("A")})
		if err = replica.Flush(context.Background()); err != nil {
			panic(err)
		}
		t2 := apply(Command{Type: CommandDelete, Key: []byte("foo")})
		t3 := apply(Command{Type: CommandPut, Key: []byte("foo"), Value: []byte("B")})
		if t1 != mvcc.Timestamp(1000<<mvcc.LogicalBits) || t2 != t1+1 || t3 != t2+1 {
			panic("unexpected timestamps")
		}
		os.Exit(79)
	}
	root := filepath.Join(t.TempDir(), "range")
	command := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestMVCCSubprocessCrashHistoricalRecovery$") //nolint:gosec // current signed test binary
	command.Env = append(os.Environ(), "RIVETDB_MVCC_CRASH_CHILD=1", "RIVETDB_MVCC_CRASH_ROOT="+root)
	if err := command.Run(); err == nil {
		t.Fatal("child did not crash")
	} else {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 79 {
			t.Fatalf("child=%v", err)
		}
	}
	replica, err := Open(Options{RangeID: 1, NodeID: 1, ReplicaID: 1, Peers: []raft.NodeID{1}, Directory: root, MVCC: true,
		Clock: clock.NewMockAt(time.UnixMilli(900)), ElectionTimeoutMin: 2, ElectionTimeoutMax: 2, HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(3, 4))})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close(context.Background())
	for replica.Status().Raft.Role != raft.Leader {
		if _, err = replica.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	t1 := mvcc.Timestamp(1000 << mvcc.LogicalBits)
	assertGetAt(t, replica, t1, "A")
	assertGetAt(t, replica, t1+1, "")
	assertGetAt(t, replica, t1+2, "B")
	next, _, _, _, err := replica.ProposeMVCC(context.Background(), Command{Type: CommandPut, Key: []byte("bar"), Value: []byte("C")})
	if err != nil || next <= t1+2 {
		t.Fatalf("post-crash timestamp=%d err=%v", next, err)
	}
}

func assertGetAt(t testing.TB, replica *Replica, timestamp mvcc.Timestamp, want string) {
	t.Helper()
	value, err := replica.GetAt(context.Background(), []byte("foo"), timestamp)
	if want == "" {
		if !errors.Is(err, engine.ErrNotFound) {
			t.Fatalf("GetAt(%d)=%q err=%v", timestamp, value, err)
		}
		return
	}
	if err != nil || string(value) != want {
		t.Fatalf("GetAt(%d)=%q err=%v want=%q", timestamp, value, err, want)
	}
}

func TestMVCCCommandCodecRejectsZeroTimestampV2(t *testing.T) {
	encoded, err := EncodeCommand(Command{Type: CommandPut, Timestamp: mvcc.Timestamp(55), Key: []byte{0, 0xff}, Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCommand(encoded)
	if err != nil || decoded.Timestamp != 55 {
		t.Fatalf("decode=%+v err=%v", decoded, err)
	}
	encoded[8] = 0
	encoded[9] = 0
	encoded[10] = 0
	encoded[11] = 0
	encoded[12] = 0
	encoded[13] = 0
	encoded[14] = 0
	encoded[15] = 0
	if _, err := DecodeCommand(encoded); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("zero timestamp=%v", err)
	}
}

func TestMVCCNewLeaderObservesUncommittedDurableTimestamp(t *testing.T) {
	cluster := newMVCCTestCluster(t, 3)
	cluster.elect(1)
	high, index, messages, _, err := cluster.replicas[1].ProposeMVCC(context.Background(), Command{Type: CommandPut, Key: []byte("uncommitted"), Value: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range messages {
		if message.To == 2 {
			if _, err = cluster.replicas[2].Step(message); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if cluster.replicas[2].Status().Raft.CommitIndex >= index {
		t.Fatal("entry unexpectedly committed")
	}
	cluster.elect(2)
	next := cluster.proposeMVCC(2, Command{Type: CommandPut, Key: []byte("after"), Value: []byte("y")})
	if next <= high {
		t.Fatalf("new leader reused uncommitted timestamp: %d <= %d", next, high)
	}
}

func TestMVCCSnapshotCountHasNoGoroutinesAndReleasesTracking(t *testing.T) {
	cluster := newMVCCTestCluster(t, 3)
	cluster.elect(1)
	cluster.proposeMVCC(1, Command{Type: CommandPut, Key: []byte("k"), Value: []byte("v")})
	before := runtime.NumGoroutine()
	snapshots := make([]*Snapshot, 1000)
	for index := range snapshots {
		var err error
		snapshots[index], err = cluster.replicas[1].NewSnapshot()
		if err != nil {
			t.Fatal(err)
		}
	}
	after := runtime.NumGoroutine()
	if after > before {
		t.Fatalf("snapshot goroutines %d -> %d", before, after)
	}
	for _, snapshot := range snapshots {
		if err := snapshot.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(cluster.replicas[1].snapshots) != 0 {
		t.Fatalf("tracked snapshots=%d", len(cluster.replicas[1].snapshots))
	}
	t.Logf("snapshots=1000 goroutines_before=%d goroutines_after=%d tracked_after_close=0", before, after)
}

func TestMVCCSnapshotFailsAfterReplicaClose(t *testing.T) {
	cluster := newMVCCTestCluster(t, 3)
	cluster.elect(1)
	cluster.proposeMVCC(1, Command{Type: CommandPut, Key: []byte("k"), Value: []byte("v")})
	snapshot, err := cluster.replicas[1].NewSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err = cluster.replicas[1].Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = snapshot.Get(context.Background(), []byte("k")); !errors.Is(err, ErrStopped) {
		t.Fatalf("snapshot after close=%v", err)
	}
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}
