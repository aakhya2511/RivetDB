package multiraft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
)

// TestPhase10RealDurableCompositionalChaos deliberately uses the existing
// filesystem-backed five-node fixture. It is lower-count than the logical
// model because every mutation traverses real Raft and MVCC LSM authorities.
func TestPhase10RealDurableCompositionalChaos(t *testing.T) {
	if os.Getenv("RIVETDB_CHAOS_DURABLE") == "" {
		t.Skip("set RIVETDB_CHAOS_DURABLE=1 for real durable Phase 10 chaos")
	}
	ctx := context.Background()
	goroutinesBefore, descriptorsBefore := runtime.NumGoroutine(), openDescriptorCount()
	cluster := newMigrationCluster(t)
	for rangeID, leader := range map[RangeID]raft.NodeID{10: 1, 11: 2, 12: 3} {
		cluster.elect(rangeID, leader)
	}
	for _, account := range []struct{ key, value string }{{"a-bank", "500"}, {"g-bank", "500"}, {"p-bank", "500"}} {
		if _, err := cluster.router.PutMVCC(ctx, []byte(account.key), []byte(account.value)); err != nil {
			t.Fatal(err)
		}
	}
	historical := make(map[string][2]uint64)
	for _, key := range []string{"a-history", "g-history", "p-history"} {
		first, err := cluster.router.PutMVCC(ctx, []byte(key), []byte("v1"))
		if err != nil {
			t.Fatal(err)
		}
		second, err := cluster.router.PutMVCC(ctx, []byte(key), []byte("v2"))
		if err != nil {
			t.Fatal(err)
		}
		historical[key] = [2]uint64{uint64(first), uint64(second)}
	}

	migration, meta := migrationManager(t, cluster)
	transferCommitted, leaderChanged, storageMaintained := false, false, false
	migration.hook = func(stage MigrationHookStage, record MigrationRecord) {
		switch stage {
		case SnapshotTargetDurable:
			replica, err := cluster.nodes[record.TargetNodeID].Replica(record.RangeID)
			if err == nil {
				if err = replica.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if err = replica.Compact(ctx); err != nil && !errors.Is(err, compaction.ErrNoCompaction) {
					t.Fatal(err)
				}
				storageMaintained = true
			}
		case LearnerAdded:
			cluster.elect(11, 3)
			leaderChanged = true
			transferBank(t, ctx, cluster, "a-bank", "g-bank", 25)
			transferCommitted = true
		}
	}
	descriptor, _ := meta.Snapshot().Catalog.LookupByID(10)
	source, _ := descriptor.ReplicaOn(3)
	migrationRecord, err := migration.MoveReplica(ctx, 10, source.ReplicaID, 4)
	if err != nil {
		t.Fatal(err)
	}
	if migrationRecord.State != MigrationSourceRetired || !transferCommitted || !leaderChanged || !storageMaintained {
		t.Fatalf("migration=%+v transfer=%v leaderChange=%v storage=%v", migrationRecord, transferCommitted, leaderChanged, storageMaintained)
	}
	if deleteErr := migration.DeleteSource(migrationRecord); deleteErr != nil {
		t.Fatal(deleteErr)
	}

	blocked, err := cluster.router.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if putErr := blocked.Put([]byte("g-fenced"), []byte("must-not-appear")); putErr != nil {
		t.Fatal(putErr)
	}
	metaFailure, safeTxn := false, false
	split, err := NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage SplitHookStage, _ SplitRecord) {
		switch stage {
		case ParentSplitFenceActive:
			if commitErr := blocked.Commit(ctx); !errors.Is(commitErr, replicatedrange.ErrSplitFenced) {
				t.Fatalf("fenced commit=%v", commitErr)
			}
		case LeftImageDurable:
			leader := meta.Leader()
			if stopErr := meta.StopNode(leader); stopErr != nil {
				t.Fatal(stopErr)
			}
			metaFailure = true
			transferBank(t, ctx, cluster, "a-bank", "p-bank", 10)
			safeTxn = true
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	splitRecord, err := split.SplitRange(ctx, 11, []byte("m"))
	if err != nil {
		t.Fatal(err)
	}
	if splitRecord.State != SplitCommitted || !metaFailure || !safeTxn {
		t.Fatalf("split=%+v metaFailure=%v txn=%v", splitRecord, metaFailure, safeTxn)
	}

	// Range-local restart and maintenance overlap the post-cutover data plane.
	if err := cluster.nodes[splitRecord.Left.Replicas[0].NodeID].RestartRange(splitRecord.Left.RangeID); err != nil {
		t.Fatal(err)
	}
	cluster.scheduler.Refresh()
	for _, current := range meta.Snapshot().Catalog.Snapshot().Ranges {
		for _, member := range current.Replicas {
			replica, lookupErr := cluster.nodes[member.NodeID].Replica(current.RangeID)
			if lookupErr != nil {
				t.Fatal(lookupErr)
			}
			if validateErr := replica.Validate(); validateErr != nil {
				t.Fatalf("range=%d node=%d: %v", current.RangeID, member.NodeID, validateErr)
			}
		}
	}
	assertBankTotal(t, ctx, cluster, 1500)
	assertHistoricalValues(t, ctx, cluster, historical)

	// Full process-model restart uses the committed metadata snapshot as the only
	// dynamic catalog authority; local directories are not adopted by existence.
	snapshot := meta.Snapshot()
	cluster.close()
	reopenSplitCluster(t, cluster, snapshot)
	for _, current := range snapshot.Catalog.Snapshot().Ranges {
		cluster.elect(current.RangeID, current.Replicas[0].NodeID)
	}
	assertBankTotal(t, ctx, cluster, 1500)
	assertHistoricalValues(t, ctx, cluster, historical)
	unexpectedTemps := auditChaosTemps(t, cluster.root)
	if unexpectedTemps != 0 {
		t.Fatalf("unexpected temporary/staging files=%d", unexpectedTemps)
	}
	cluster.close()
	runtime.GC()
	goroutinesAfter, descriptorsAfter := runtime.NumGoroutine(), openDescriptorCount()
	if goroutinesAfter > goroutinesBefore+8 {
		t.Fatalf("goroutine growth before=%d after=%d", goroutinesBefore, goroutinesAfter)
	}
	if descriptorsBefore >= 0 && descriptorsAfter > descriptorsBefore+8 {
		t.Fatalf("descriptor growth before=%d after=%d", descriptorsBefore, descriptorsAfter)
	}
	t.Logf("nodes=5 initialRanges=3 finalRanges=%d transactions=2 migration=1 jointConfigs=1 split=1 nodeRestarts=1 fullClusterRestarts=1 MetaRangeLeaderFailures=1 historicalChecks=6 bankTotal=1500 goroutines=%d/%d descriptors=%d/%d unexpectedTemps=0 root=%s",
		len(snapshot.Catalog.Snapshot().Ranges), goroutinesBefore, goroutinesAfter, descriptorsBefore, descriptorsAfter, cluster.root)
}

func transferBank(t testing.TB, ctx context.Context, cluster *multiTestCluster, from, to string, amount int) {
	t.Helper()
	transaction, err := cluster.router.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fromRaw, err := transaction.Get(ctx, []byte(from))
	if err != nil {
		t.Fatal(err)
	}
	toRaw, err := transaction.Get(ctx, []byte(to))
	if err != nil {
		t.Fatal(err)
	}
	fromValue, err := strconv.Atoi(string(fromRaw))
	if err != nil {
		t.Fatal(err)
	}
	toValue, err := strconv.Atoi(string(toRaw))
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Put([]byte(from), []byte(strconv.Itoa(fromValue-amount))); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Put([]byte(to), []byte(strconv.Itoa(toValue+amount))); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func assertBankTotal(t testing.TB, ctx context.Context, cluster *multiTestCluster, expected int) {
	t.Helper()
	transaction, err := cluster.router.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, key := range []string{"a-bank", "g-bank", "p-bank"} {
		value, getErr := transaction.Get(ctx, []byte(key))
		if getErr != nil {
			t.Fatal(getErr)
		}
		parsed, parseErr := strconv.Atoi(string(value))
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		total += parsed
	}
	if err := transaction.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	if total != expected {
		t.Fatalf("bank total=%d want=%d", total, expected)
	}
}

func assertHistoricalValues(t testing.TB, ctx context.Context, cluster *multiTestCluster, timestamps map[string][2]uint64) {
	t.Helper()
	for key, pair := range timestamps {
		descriptor, err := cluster.router.currentCatalog().Lookup([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		replica, err := cluster.router.leaderReplica(ctx, descriptor)
		if err != nil {
			t.Fatal(err)
		}
		for index, raw := range pair {
			value, getErr := replica.GetAt(ctx, []byte(key), mvcc.Timestamp(raw))
			if getErr != nil {
				t.Fatal(getErr)
			}
			if want := fmt.Sprintf("v%d", index+1); string(value) != want {
				t.Fatalf("key=%s at=%d value=%q want=%q", key, raw, value, want)
			}
		}
	}
}

func auditChaosTemps(t testing.TB, root string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		name := strings.ToLower(entry.Name())
		if strings.Contains(name, ".tmp") || strings.Contains(name, "staging") || strings.Contains(name, "partial") {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func openDescriptorCount() int {
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}
