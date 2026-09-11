package replicatedrange

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
)

func TestReplicatedDurabilityStageOrder(t *testing.T) {
	var mu sync.Mutex
	var observed []Stage
	replica, err := Open(Options{
		RangeID: StaticRangeID, NodeID: 1, ReplicaID: 1, Peers: []raft.NodeID{1},
		Directory: t.TempDir(), ElectionTimeoutMin: 2, ElectionTimeoutMax: 2,
		HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(1, 2)),
		Engine: engine.Options{MemTableBytes: 1 << 20},
		Hook: func(stage Stage, _ uint64) {
			mu.Lock()
			observed = append(observed, stage)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close(context.Background()) })
	for replica.Status().Raft.Role != raft.Leader {
		if _, tickErr := replica.Tick(); tickErr != nil {
			t.Fatal(tickErr)
		}
	}
	encoded, err := EncodeCommand(Command{Type: CommandPut, Key: []byte("key"), Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	index, _, waiter, err := replica.Propose(context.Background(), encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := replica.Await(context.Background(), index, waiter); err != nil {
		t.Fatal(err)
	}
	if err := replica.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]Stage(nil), observed...)
	mu.Unlock()
	want := []Stage{
		StageRaftCommitted, StageApplyStarted, StageMemTableApplied,
		StageVisibilityPublished, StageSSTableDurable,
		StageManifestAppliedFrontierDurable,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("stages=%v want=%v", got, want)
	}
}

type testCluster struct {
	t        testing.TB
	root     string
	ids      []raft.NodeID
	replicas map[raft.NodeID]*Replica
	hookFor  func(raft.NodeID) Hook
	mvcc     bool
	clocks   map[raft.NodeID]clock.Clock
}

func newTestCluster(t testing.TB, size int) *testCluster {
	t.Helper()
	return newTestClusterAt(t, size, t.TempDir())
}

func newMVCCTestCluster(t testing.TB, size int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, root: t.TempDir(), replicas: make(map[raft.NodeID]*Replica), mvcc: true, clocks: make(map[raft.NodeID]clock.Clock)}
	for id := 1; id <= size; id++ {
		nodeID := raft.NodeID(id)
		c.ids = append(c.ids, nodeID)
		physicalMillis := map[raft.NodeID]int64{1: 5000, 2: 1000, 3: 3000, 4: 2000, 5: 4000}[nodeID]
		c.clocks[nodeID] = clock.NewMockAt(time.UnixMilli(physicalMillis))
	}
	c.openAll()
	t.Cleanup(func() { c.closeAll() })
	return c
}

func newTestClusterAt(t testing.TB, size int, root string) *testCluster {
	return newTestClusterAtWithHook(t, size, root, nil)
}

func newTestClusterAtWithHook(t testing.TB, size int, root string, hookFor func(raft.NodeID) Hook) *testCluster {
	t.Helper()
	c := &testCluster{t: t, root: root, replicas: make(map[raft.NodeID]*Replica), hookFor: hookFor}
	for id := 1; id <= size; id++ {
		c.ids = append(c.ids, raft.NodeID(id))
	}
	c.openAll()
	t.Cleanup(func() { c.closeAll() })
	return c
}

func (c *testCluster) openAll() {
	c.t.Helper()
	for _, id := range c.ids {
		if c.replicas[id] != nil {
			continue
		}
		rangeRoot := filepath.Join(c.root, fmt.Sprintf("node-%d", id), "ranges", "1")
		var hook Hook
		if c.hookFor != nil {
			hook = c.hookFor(id)
		}
		replica, err := Open(Options{
			RangeID: StaticRangeID, NodeID: id, ReplicaID: ReplicaID(id), Peers: c.ids,
			Directory: rangeRoot, ElectionTimeoutMin: 3 + uint64(id), ElectionTimeoutMax: 3 + uint64(id),
			HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(uint64(id), uint64(id)+99)),
			Engine: engine.Options{MemTableBytes: 1 << (10 + id)},
			Hook:   hook,
			MVCC:   c.mvcc, Clock: c.clocks[id],
		})
		if err != nil {
			c.t.Fatalf("open node %d: %v", id, err)
		}
		c.replicas[id] = replica
	}
}

func (c *testCluster) proposeMVCC(leader raft.NodeID, command Command) mvcc.Timestamp {
	c.t.Helper()
	timestamp, index, messages, waiter, err := c.replicas[leader].ProposeMVCC(context.Background(), command)
	if err != nil {
		c.t.Fatal(err)
	}
	c.deliver(messages, 10000)
	if awaitErr := c.replicas[leader].Await(context.Background(), index, waiter); awaitErr != nil {
		c.t.Fatal(awaitErr)
	}
	for rounds := 0; rounds < 2; rounds++ {
		messages, err = c.replicas[leader].Tick()
		if err != nil {
			c.t.Fatal(err)
		}
		c.deliver(messages, 10000)
	}
	return timestamp
}

func (c *testCluster) closeAll() {
	for id, replica := range c.replicas {
		if replica != nil {
			if err := replica.Close(context.Background()); err != nil {
				c.t.Errorf("close node %d: %v", id, err)
			}
			c.replicas[id] = nil
		}
	}
}

func (c *testCluster) elect(id raft.NodeID) {
	c.t.Helper()
	for attempts := 0; attempts < 30 && c.replicas[id].Status().Raft.Role != raft.Leader; attempts++ {
		messages, err := c.replicas[id].Tick()
		if err != nil {
			c.t.Fatal(err)
		}
		c.deliver(messages, 10000)
	}
	if c.replicas[id].Status().Raft.Role != raft.Leader {
		c.t.Fatalf("node %d not elected: %+v", id, c.replicas[id].Status())
	}
	// Carry the leader no-op commit to every follower.
	for rounds := 0; rounds < 3; rounds++ {
		messages, err := c.replicas[id].Tick()
		if err != nil {
			c.t.Fatal(err)
		}
		c.deliver(messages, 10000)
	}
}

func (c *testCluster) deliver(initial []raft.Message, limit int) {
	c.t.Helper()
	queue := append([]raft.Message(nil), initial...)
	for len(queue) != 0 {
		if limit == 0 {
			c.t.Fatal("delivery limit")
		}
		limit--
		message := queue[0]
		queue = queue[1:]
		target := c.replicas[message.To]
		if target == nil {
			continue
		}
		out, err := target.Step(message)
		if err != nil {
			c.t.Fatalf("deliver %v: %v", message.Type, err)
		}
		queue = append(queue, out...)
	}
}

func (c *testCluster) propose(leader raft.NodeID, command Command) uint64 {
	c.t.Helper()
	encoded, err := EncodeCommand(command)
	if err != nil {
		c.t.Fatal(err)
	}
	index, messages, waiter, err := c.replicas[leader].Propose(context.Background(), encoded)
	if err != nil {
		c.t.Fatal(err)
	}
	c.deliver(messages, 10000)
	if err := c.replicas[leader].Await(context.Background(), index, waiter); err != nil {
		c.t.Fatal(err)
	}
	for rounds := 0; rounds < 2; rounds++ {
		messages, tickErr := c.replicas[leader].Tick()
		if tickErr != nil {
			c.t.Fatal(tickErr)
		}
		c.deliver(messages, 10000)
	}
	return index
}

func (c *testCluster) assertConverged() {
	c.t.Helper()
	var expected [32]byte
	var applied uint64
	for offset, id := range c.ids {
		replica := c.replicas[id]
		if err := replica.Validate(); err != nil {
			c.t.Fatalf("node %d validate: %v", id, err)
		}
		digest, err := replica.Digest(context.Background())
		if err != nil {
			c.t.Fatal(err)
		}
		status := replica.Status()
		if offset == 0 {
			expected, applied = digest, status.Raft.LastApplied
			continue
		}
		if digest != expected || status.Raft.LastApplied != applied {
			c.t.Fatalf("node %d digest=%x applied=%d want=%x/%d", id, digest, status.Raft.LastApplied, expected, applied)
		}
	}
}

func TestThreeAndFiveNodeReplicatedRangeConvergence(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprintf("nodes-%d", size), func(t *testing.T) {
			cluster := newTestCluster(t, size)
			cluster.elect(1)
			var last uint64
			for index := 0; index < 80; index++ {
				command := Command{Type: CommandPut, Key: []byte(fmt.Sprintf("key-%02d", index%19)), Value: []byte(fmt.Sprintf("value-%03d", index))}
				if index%7 == 0 {
					command.Type, command.Value = CommandDelete, nil
				}
				last = cluster.propose(1, command)
				if index%11 == 0 {
					if err := cluster.replicas[1].Flush(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				if index%17 == 0 && size > 2 {
					if err := cluster.replicas[2].Flush(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
			}
			cluster.assertConverged()
			if cluster.replicas[1].Status().Raft.LastApplied != last {
				t.Fatalf("last applied != command: %+v last=%d", cluster.replicas[1].Status(), last)
			}
			if cluster.replicas[1].Status().LiveTableCount == cluster.replicas[3].Status().LiveTableCount {
				t.Fatalf("physical layouts did not diverge: one=%+v three=%+v", cluster.replicas[1].Status(), cluster.replicas[3].Status())
			}
			t.Logf("nodes=%d commands=80 final_applied=%d live_tables=[%d,%d,%d] digest_mismatches=0", size, last,
				cluster.replicas[1].Status().LiveTableCount, cluster.replicas[2].Status().LiveTableCount, cluster.replicas[3].Status().LiveTableCount)
		})
	}
}

func TestUncommittedEntryNeverMutatesLSMAndWaiterFailsOnStepdown(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.elect(1)
	encoded, err := EncodeCommand(Command{Type: CommandPut, Key: []byte("minority"), Value: []byte("unsafe")})
	if err != nil {
		t.Fatal(err)
	}
	index, _, waiter, err := cluster.replicas[1].Propose(context.Background(), encoded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.replicas[1].LocalGet(context.Background(), []byte("minority")); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("uncommitted command reached LSM: %v", err)
	}
	status := cluster.replicas[1].Status().Raft
	_, _ = cluster.replicas[1].Step(raft.Message{Type: raft.RequestVote, From: 2, To: 1, Term: status.Term + 1})
	if err := cluster.replicas[1].Await(context.Background(), index, waiter); !errors.Is(err, ErrLeadershipLost) {
		t.Fatalf("waiter error=%v", err)
	}
}

func TestFullClusterRestartReplaysUnflushedCommittedState(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.elect(1)
	for index := 0; index < 30; index++ {
		cluster.propose(1, Command{Type: CommandPut, Key: []byte(fmt.Sprintf("k-%02d", index)), Value: []byte("v")})
	}
	before, err := cluster.replicas[1].Digest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cluster.closeAll()
	cluster.openAll()
	cluster.elect(1)
	cluster.assertConverged()
	after, err := cluster.replicas[1].Digest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(before[:], after[:]) {
		// The digest includes applied index; the recovery election appends a no-op.
		t.Fatal("expected applied-index component to change after recovery election")
	}
	values, err := cluster.replicas[1].LocalScan(context.Background())
	if err != nil || len(values) != 30 {
		t.Fatalf("restored values=%d err=%v", len(values), err)
	}
}

func TestFullRestartWithDifferentDurableFrontiers(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.elect(1)
	for index := 0; index < 10; index++ {
		cluster.propose(1, Command{Type: CommandPut, Key: []byte(fmt.Sprintf("k-%02d", index)), Value: []byte("first")})
	}
	if err := cluster.replicas[1].Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := 10; index < 20; index++ {
		cluster.propose(1, Command{Type: CommandPut, Key: []byte(fmt.Sprintf("k-%02d", index)), Value: []byte("second")})
	}
	if err := cluster.replicas[2].Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := 20; index < 30; index++ {
		cluster.propose(1, Command{Type: CommandPut, Key: []byte(fmt.Sprintf("k-%02d", index)), Value: []byte("third")})
	}
	frontiers := []uint64{cluster.replicas[1].Status().DurableAppliedRaftIndex, cluster.replicas[2].Status().DurableAppliedRaftIndex, cluster.replicas[3].Status().DurableAppliedRaftIndex}
	if frontiers[0] <= frontiers[1] || frontiers[1] <= frontiers[2] {
		t.Fatalf("frontiers did not diverge: %v", frontiers)
	}
	cluster.closeAll()
	cluster.openAll()
	cluster.elect(1)
	cluster.assertConverged()
	replays := []uint64{cluster.replicas[1].Status().MaterializedCommands, cluster.replicas[2].Status().MaterializedCommands, cluster.replicas[3].Status().MaterializedCommands}
	if replays[0] >= replays[1] || replays[1] >= replays[2] {
		t.Fatalf("replay counts do not reflect frontiers: frontiers=%v replay=%v", frontiers, replays)
	}
	t.Logf("frontiers_before=%v replayed_commands=%v digest_mismatches=0", frontiers, replays)
}

func TestLeaderCrashImmediatelyAfterSuccessPreservesMutation(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.elect(1)
	index := cluster.propose(1, Command{Type: CommandPut, Key: []byte("survives"), Value: []byte("yes")})
	if cluster.replicas[1].Status().Raft.LastApplied < index {
		t.Fatal("success preceded local apply")
	}
	if err := cluster.replicas[1].Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	cluster.replicas[1] = nil
	cluster.elect(2)
	value, err := cluster.replicas[2].LocalGet(context.Background(), []byte("survives"))
	if err != nil || string(value) != "yes" {
		t.Fatalf("value=%q err=%v", value, err)
	}
}

func TestProposalCancellationAndShutdownDoNotLeakWaiters(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.elect(1)
	encoded, _ := EncodeCommand(Command{Type: CommandPut, Key: []byte("eventual"), Value: []byte("value")})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := cluster.replicas[1].Propose(ctx, encoded); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-admission cancel=%v", err)
	}
	index, messages, waiter, err := cluster.replicas[1].Propose(context.Background(), encoded)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithCancel(context.Background())
	waitCancel()
	if waitErr := cluster.replicas[1].Await(waitCtx, index, waiter); !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("wait cancel=%v", waitErr)
	}
	cluster.deliver(messages, 10000)
	value, err := cluster.replicas[1].LocalGet(context.Background(), []byte("eventual"))
	if err != nil || string(value) != "value" {
		t.Fatalf("cancelled client suppressed command value=%q err=%v", value, err)
	}
	index, _, waiter, err = cluster.replicas[1].Propose(context.Background(), encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.replicas[1].Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	cluster.replicas[1] = nil
	if result := <-waiter; result.Index != index || !errors.Is(result.Err, ErrStopped) {
		t.Fatalf("shutdown waiter=%+v", result)
	}
}

func TestMalformedCommittedCommandIsFatal(t *testing.T) {
	directory := t.TempDir()
	store, err := raft.NewMemoryStoreFrom(raft.PersistentState{HardState: raft.HardState{Term: 1}, Entries: []raft.Entry{{Index: 1, Term: 1, Type: raft.EntryCommand, Command: []byte("bad")}}})
	if err != nil {
		t.Fatal(err)
	}
	replica, err := Open(Options{RangeID: 1, NodeID: 2, ReplicaID: 2, Peers: []raft.NodeID{1, 2, 3}, Directory: directory, Store: store,
		ElectionTimeoutMin: 10, ElectionTimeoutMax: 10, HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(1, 2))})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close(context.Background())
	_, err = replica.Step(raft.Message{Type: raft.AppendEntries, From: 1, To: 2, Term: 1, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 1})
	if !errors.Is(err, raft.ErrApply) || replica.Status().Raft.Fatal == nil {
		t.Fatalf("err=%v status=%+v", err, replica.Status())
	}
}

func TestRangeKeyValidatorRejectsAdmissionAndCommittedApply(t *testing.T) {
	directory := t.TempDir()
	local, err := engine.Open(engine.Options{Directory: filepath.Join(directory, "data"), Mode: engine.ModeReplicated})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := local.Close(context.Background()); closeErr != nil {
			t.Fatal(closeErr)
		}
	}()
	machine := &stateMachine{engine: local, containsKey: func(key []byte) bool { return bytes.Compare(key, []byte("m")) < 0 }}
	encoded, err := EncodeCommand(Command{Type: CommandPut, Key: []byte("z"), Value: []byte("outside")})
	if err != nil {
		t.Fatal(err)
	}
	if applyErr := machine.Apply(raft.Entry{Index: 1, Term: 1, Type: raft.EntryCommand, Command: encoded}); !errors.Is(applyErr, ErrKeyOutOfRange) {
		t.Fatalf("committed ownership check=%v", applyErr)
	}
	replica, err := Open(Options{RangeID: 9, NodeID: 1, ReplicaID: 1, Peers: []raft.NodeID{1}, Directory: filepath.Join(directory, "replica"),
		ElectionTimeoutMin: 3, ElectionTimeoutMax: 3, HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(1, 2)), ContainsKey: machine.containsKey})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := replica.Close(context.Background()); closeErr != nil {
			t.Fatal(closeErr)
		}
	}()
	if _, _, _, err := replica.Propose(context.Background(), encoded); !errors.Is(err, ErrKeyOutOfRange) {
		t.Fatalf("admission ownership check=%v", err)
	}
}

func TestMajorityContinuesAfterFollowerStorageApplyFailure(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.elect(1)
	failedReplica := cluster.replicas[3]
	if err := failedReplica.engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeCommand(Command{Type: CommandPut, Key: []byte("first"), Value: []byte("committed")})
	if err != nil {
		t.Fatal(err)
	}
	index, initial, waiter, err := cluster.replicas[1].Propose(context.Background(), encoded)
	if err != nil {
		t.Fatal(err)
	}
	queue := append([]raft.Message(nil), initial...)
	storageFailed := false
	for len(queue) != 0 {
		message := queue[0]
		queue = queue[1:]
		target := cluster.replicas[message.To]
		if target == nil {
			continue
		}
		out, stepErr := target.Step(message)
		if message.To == 3 && stepErr != nil {
			storageFailed = true
			cluster.replicas[3] = nil
			continue
		}
		if stepErr != nil {
			t.Fatalf("healthy node step: %v", stepErr)
		}
		queue = append(queue, out...)
	}
	if !storageFailed || failedReplica.Status().Raft.Fatal == nil {
		t.Fatalf("failed replica remained healthy: %+v", failedReplica.Status())
	}
	if err := cluster.replicas[1].Await(context.Background(), index, waiter); err != nil {
		t.Fatal(err)
	}
	cluster.propose(1, Command{Type: CommandPut, Key: []byte("second"), Value: []byte("also-committed")})
	for _, id := range []raft.NodeID{1, 2} {
		value, getErr := cluster.replicas[id].LocalGet(context.Background(), []byte("second"))
		if getErr != nil || string(value) != "also-committed" {
			t.Fatalf("node %d value=%q err=%v", id, value, getErr)
		}
	}
}

func TestReplicatedRangeSubprocessCrashRecoveryAtEveryApplyStage(t *testing.T) {
	if os.Getenv("RIVETDB_RANGE_CRASH") == "" {
		t.Skip("set RIVETDB_RANGE_CRASH=1 for process crash recovery")
	}
	if root := os.Getenv("RIVETDB_RANGE_CRASH_CHILD"); root != "" {
		target := os.Getenv("RIVETDB_RANGE_CRASH_STAGE")
		cluster := newTestClusterAtWithHook(t, 3, root, func(id raft.NodeID) Hook {
			if id != 1 {
				return nil
			}
			return func(stage Stage, _ uint64) {
				if stage.String() == target {
					os.Exit(77)
				}
			}
		})
		cluster.elect(1)
		cluster.propose(1, Command{Type: CommandPut, Key: []byte("process-crash"), Value: []byte("survived")})
		if target == StageSSTableDurable.String() || target == StageManifestAppliedFrontierDurable.String() {
			if err := cluster.replicas[1].Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		t.Fatalf("stage %s did not crash", target)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	stages := []Stage{
		StageRaftCommitted, StageApplyStarted, StageMemTableApplied,
		StageVisibilityPublished, StageSSTableDurable,
		StageManifestAppliedFrontierDurable,
	}
	for _, stage := range stages {
		t.Run(stage.String(), func(t *testing.T) {
			root := t.TempDir()
			command := exec.CommandContext(context.Background(), executable, "-test.run=^TestReplicatedRangeSubprocessCrashRecoveryAtEveryApplyStage$") //nolint:gosec // current signed test binary
			command.Env = append(os.Environ(), "RIVETDB_RANGE_CRASH=1", "RIVETDB_RANGE_CRASH_CHILD="+root, "RIVETDB_RANGE_CRASH_STAGE="+stage.String())
			runErr := command.Run()
			if runErr == nil {
				t.Fatal("child returned without crashing")
			} else {
				var exit *exec.ExitError
				if !errors.As(runErr, &exit) || exit.ExitCode() != 77 {
					t.Fatalf("child error=%v", runErr)
				}
			}
			cluster := newTestClusterAt(t, 3, root)
			cluster.elect(1)
			cluster.assertConverged()
			value, getErr := cluster.replicas[1].LocalGet(context.Background(), []byte("process-crash"))
			if getErr != nil || string(value) != "survived" {
				t.Fatalf("stage=%s recovered value=%q err=%v", stage, value, getErr)
			}
		})
	}
}
