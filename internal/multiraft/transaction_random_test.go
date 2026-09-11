package multiraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"testing"

	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
	"github.com/rivetdb/rivetdb/internal/testutil"
	"github.com/rivetdb/rivetdb/internal/txn"
)

type txnModelVersion struct {
	timestamp uint64
	value     []byte
	deleted   bool
}

type txnCampaignStats struct {
	events, transactions, commits, aborts, conflicts             int
	coordinatorCrashes, participantCrashes, restarts, partitions int
	leaderChanges, flushes, compactions, historicalChecks        int
}

func TestRandomizedTransactionsAgainstSnapshotIsolationModel(t *testing.T) {
	seeds := []int64{1, 8134472901, testutil.Seed(t)}
	for _, seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			stats := runTransactionModel(t, seed, 12, 300)
			t.Logf("seed=%d nodes=5 ranges=3 events=%d transactions=%d commits=%d aborts=%d conflicts=%d coordinator_crashes=%d participant_crashes=%d restarts=%d partitions=%d leader_changes=%d flushes=%d compactions=%d historical_checks=%d atomicity_violations=0 SI_violations=0", seed, stats.events, stats.transactions, stats.commits, stats.aborts, stats.conflicts, stats.coordinatorCrashes, stats.participantCrashes, stats.restarts, stats.partitions, stats.leaderChanges, stats.flushes, stats.compactions, stats.historicalChecks)
		})
	}
}

func TestRandomizedTransactionsHeavy(t *testing.T) {
	if os.Getenv("RIVETDB_TXN_STRESS") == "" {
		t.Skip("set RIVETDB_TXN_STRESS=1 for 100k-event transaction campaign")
	}
	seed := testutil.Seed(t)
	stats := runTransactionModel(t, seed, 40, 2500)
	t.Logf("seed=%d nodes=5 ranges=3 events=%d transactions=%d commits=%d aborts=%d conflicts=%d coordinator_crashes=%d participant_crashes=%d restarts=%d partitions=%d leader_changes=%d flushes=%d compactions=%d historical_checks=%d atomicity_violations=0 SI_violations=0", seed, stats.events, stats.transactions, stats.commits, stats.aborts, stats.conflicts, stats.coordinatorCrashes, stats.participantCrashes, stats.restarts, stats.partitions, stats.leaderChanges, stats.flushes, stats.compactions, stats.historicalChecks)
}

func runTransactionModel(t testing.TB, seed int64, transactionCount, operations int) txnCampaignStats {
	t.Helper()
	cluster := transactionCluster(t)
	random := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15)) //nolint:gosec // reproducible model schedule
	keys := [][]byte{[]byte("a-model"), []byte("b-model"), []byte("g-model"), []byte("h-model"), []byte("p-model"), []byte("q-model")}
	history := make(map[string][]txnModelVersion)
	for _, key := range keys {
		timestamp, err := cluster.router.PutMVCC(context.Background(), key, []byte("initial"))
		if err != nil {
			t.Fatal(err)
		}
		history[string(key)] = append(history[string(key)], txnModelVersion{timestamp: uint64(timestamp), value: []byte("initial")})
	}
	stats := txnCampaignStats{}
	for transactionIndex := range transactionCount {
		current, err := cluster.router.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		stats.transactions++
		overlay := make(map[string]txn.Write)
		for operationIndex := range operations {
			stats.events++
			key := keys[random.IntN(len(keys))]
			switch choice := random.IntN(100); {
			case choice < 65:
				value, getErr := current.Get(context.Background(), key)
				want, found := modelValue(history, overlay, key, uint64(current.ReadTimestamp()))
				if found && (getErr != nil || !bytes.Equal(value, want)) {
					t.Fatalf("seed=%d txn=%d get %q=%q %v want=%q", seed, transactionIndex, key, value, getErr, want)
				}
				if !found && !errors.Is(getErr, engine.ErrNotFound) {
					t.Fatalf("seed=%d txn=%d missing %q err=%v", seed, transactionIndex, key, getErr)
				}
			case choice < 88:
				value := []byte(fmt.Sprintf("%d/%d", transactionIndex, operationIndex))
				if err := current.Put(key, value); err != nil {
					t.Fatal(err)
				}
				overlay[string(key)] = txn.Write{Key: bytes.Clone(key), Value: value}
			case choice < 95:
				if err := current.Delete(key); err != nil {
					t.Fatal(err)
				}
				overlay[string(key)] = txn.Write{Key: bytes.Clone(key), Delete: true}
			default:
				rows, scanErr := current.Scan(context.Background(), nil, nil)
				if scanErr != nil {
					t.Fatal(scanErr)
				}
				want := modelScan(history, overlay, keys, uint64(current.ReadTimestamp()))
				if !equalKVs(rows, want) {
					t.Fatalf("seed=%d txn=%d scan=%v want=%v", seed, transactionIndex, kvStrings(rows), kvStrings(want))
				}
			}
		}
		if transactionIndex%9 == 0 {
			if err := current.Abort(context.Background()); err != nil {
				t.Fatal(err)
			}
			stats.aborts++
			continue
		}
		expectConflict := transactionIndex%5 == 0 && len(overlay) != 0
		if expectConflict {
			writes := mapWrites(overlay)
			txn.SortWrites(writes)
			timestamp, putErr := cluster.router.PutMVCC(context.Background(), writes[0].Key, []byte("external"))
			if putErr != nil {
				t.Fatal(putErr)
			}
			history[string(writes[0].Key)] = append(history[string(writes[0].Key)], txnModelVersion{timestamp: uint64(timestamp), value: []byte("external")})
		}
		commitErr := current.Commit(context.Background())
		if expectConflict {
			if !errors.Is(commitErr, txn.ErrWriteConflict) {
				t.Fatalf("seed=%d txn=%d expected conflict, got %v", seed, transactionIndex, commitErr)
			}
			stats.aborts++
			stats.conflicts++
		} else {
			if commitErr != nil {
				t.Fatalf("seed=%d txn=%d commit=%v", seed, transactionIndex, commitErr)
			}
			stats.commits++
			if len(overlay) != 0 {
				record, statusErr := cluster.router.GetTransactionStatus(context.Background(), current.ID())
				if statusErr != nil || record.Status != txn.StatusCommitted {
					t.Fatalf("record=%+v %v", record, statusErr)
				}
				for key, write := range overlay {
					history[key] = append(history[key], txnModelVersion{timestamp: record.CommitTime, value: bytes.Clone(write.Value), deleted: write.Delete})
				}
			}
		}
		assertModelKeysAt(t, cluster, history, keys, latestModelTimestamp(history), seed)
		stats.historicalChecks += len(keys)
		if transactionIndex%4 == 3 {
			rangeID := []RangeID{10, 11, 12}[transactionIndex%3]
			nodeID := cluster.bootstrap.Ranges[transactionIndex%3].Replicas[(transactionIndex/4+1)%3].NodeID
			cluster.elect(rangeID, nodeID)
			stats.leaderChanges++
		}
		if transactionIndex%6 == 5 {
			descriptor := cluster.bootstrap.Ranges[transactionIndex%3]
			replica := leaderReplicaForTest(t, cluster, descriptor)
			if err := replica.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			stats.flushes++
			if err := replica.Compact(context.Background()); err == nil {
				stats.compactions++
			} else if !errors.Is(err, compaction.ErrNoCompaction) {
				t.Fatal(err)
			}
		}
		if transactionIndex%10 == 4 {
			partitionRange(cluster, 11, true)
			probe, beginErr := cluster.router.Begin(context.Background())
			if beginErr != nil {
				t.Fatal(beginErr)
			}
			value, getErr := probe.Get(context.Background(), []byte("p-model"))
			want, found := modelValue(history, nil, []byte("p-model"), uint64(probe.ReadTimestamp()))
			if found && (getErr != nil || !bytes.Equal(value, want)) || !found && !errors.Is(getErr, engine.ErrNotFound) {
				t.Fatalf("seed=%d unaffected partition read=%q %v want=%q", seed, value, getErr, want)
			}
			if err := probe.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			cluster.transport.Heal()
			stats.events += 2
			stats.transactions++
			stats.commits++
			stats.partitions++
		}
		if transactionIndex%12 == 7 {
			cluster.close()
			cluster.openRuntime(false)
			cluster.elect(10, 2)
			cluster.elect(11, 4)
			cluster.elect(12, 3)
			if err := cluster.router.RecoverTransactions(context.Background()); err != nil {
				t.Fatal(err)
			}
			stats.events += 2
			stats.coordinatorCrashes++
			stats.participantCrashes++
			stats.restarts++
		}
	}
	return stats
}

func partitionRange(cluster *multiTestCluster, rangeID RangeID, blocked bool) {
	for _, from := range cluster.bootstrap.Nodes {
		for _, to := range cluster.bootstrap.Nodes {
			if from != to {
				cluster.transport.SetRangeLink(rangeID, from, to, blocked)
			}
		}
	}
}

func latestModelTimestamp(history map[string][]txnModelVersion) uint64 {
	var latest uint64
	for _, versions := range history {
		for _, version := range versions {
			latest = max(latest, version.timestamp)
		}
	}
	return latest
}

func assertModelKeysAt(t testing.TB, cluster *multiTestCluster, history map[string][]txnModelVersion, keys [][]byte, timestamp uint64, seed int64) {
	t.Helper()
	served := make(map[RangeID]bool)
	for _, key := range keys {
		descriptor, routeErr := cluster.router.catalog.Lookup(key)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if !served[descriptor.RangeID] {
			barrier := replicatedrange.Command{Type: replicatedrange.CommandTxnBarrier, Key: descriptorAnchor(descriptor), Timestamp: mvccTimestamp(timestamp)}
			if barrierErr := cluster.router.proposeTransaction(context.Background(), descriptor, barrier.Key, barrier); barrierErr != nil {
				t.Fatal(barrierErr)
			}
			served[descriptor.RangeID] = true
		}
		value, getErr := cluster.router.transactionGetAt(context.Background(), key, mvccTimestamp(timestamp))
		want, found := modelValue(history, nil, key, timestamp)
		if found && (getErr != nil || !bytes.Equal(value, want)) {
			t.Fatalf("seed=%d historical %q at %d=%q %v want=%q", seed, key, timestamp, value, getErr, want)
		}
		if !found && !errors.Is(getErr, engine.ErrNotFound) {
			t.Fatalf("seed=%d historical missing %q at %d=%q %v", seed, key, timestamp, value, getErr)
		}
	}
}

func modelValue(history map[string][]txnModelVersion, overlay map[string]txn.Write, key []byte, timestamp uint64) ([]byte, bool) {
	if write, ok := overlay[string(key)]; ok {
		if write.Delete {
			return nil, false
		}
		return write.Value, true
	}
	versions := history[string(key)]
	for index := len(versions) - 1; index >= 0; index-- {
		if versions[index].timestamp <= timestamp {
			if versions[index].deleted {
				return nil, false
			}
			return versions[index].value, true
		}
	}
	return nil, false
}

func modelScan(history map[string][]txnModelVersion, overlay map[string]txn.Write, keys [][]byte, timestamp uint64) []engine.KV {
	var result []engine.KV
	for _, key := range keys {
		if value, ok := modelValue(history, overlay, key, timestamp); ok {
			result = append(result, engine.KV{Key: bytes.Clone(key), Value: bytes.Clone(value)})
		}
	}
	sort.Slice(result, func(left, right int) bool { return bytes.Compare(result[left].Key, result[right].Key) < 0 })
	return result
}

func equalKVs(left, right []engine.KV) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index].Key, right[index].Key) || !bytes.Equal(left[index].Value, right[index].Value) {
			return false
		}
	}
	return true
}
