package multiraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

type mvccModelVersion struct {
	timestamp mvcc.Timestamp
	deleted   bool
	value     []byte
}

func TestRandomizedMultiRaftMVCC(t *testing.T) {
	for _, seed := range []int64{511, 512, testutil.Seed(t)} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) { runMultiRaftMVCCModel(t, seed, 10_000) })
	}
}

func TestRandomizedMultiRaftMVCCHeavy(t *testing.T) {
	if os.Getenv("RIVETDB_MVCC_STRESS") != "1" {
		t.Skip("set RIVETDB_MVCC_STRESS=1")
	}
	runMultiRaftMVCCModel(t, 517, 100_000)
}

func runMultiRaftMVCCModel(t testing.TB, seed int64, events int) {
	t.Helper()
	cluster := newMVCCMultiTestCluster(t)
	leaders := map[RangeID]raft.NodeID{10: 1, 11: 3, 12: 5}
	for rid, leader := range leaders {
		cluster.elect(rid, leader)
	}
	rng := testutil.RandFromSeed(seed)
	history := make(map[RangeID]map[string][]mvccModelVersion)
	for _, rid := range []RangeID{10, 11, 12} {
		history[rid] = make(map[string][]mvccModelVersion)
	}
	stats := struct{ puts, deletes, gets, scans, snapshots, leaders, partitions, rangeRestarts, nodeCrashes, nodeRestarts, flushes, compactions int }{}
	for event := 0; event < events; event++ {
		descriptor := cluster.bootstrap.Ranges[rng.IntN(3)]
		rid := descriptor.RangeID
		key := mvccRangeKey(rid, rng.IntN(12))
		switch {
		case event%97 == 0:
			var timestamp mvcc.Timestamp
			var err error
			deleted := event%679 == 0
			if deleted {
				timestamp, err = cluster.router.DeleteMVCC(context.Background(), key)
				stats.deletes++
			} else {
				value := []byte(fmt.Sprintf("%d/%d", seed, event))
				timestamp, err = cluster.router.PutMVCC(context.Background(), key, value)
				stats.puts++
				if err != nil {
					t.Fatal(err)
				}
				history[rid][string(key)] = append(history[rid][string(key)], mvccModelVersion{timestamp: timestamp, value: bytes.Clone(value)})
				driveMVCCLeader(t, cluster, rid, leaders[rid])
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			history[rid][string(key)] = append(history[rid][string(key)], mvccModelVersion{timestamp: timestamp, deleted: true})
			driveMVCCLeader(t, cluster, rid, leaders[rid])
		case event%101 == 0:
			leader := cluster.nodes[leaders[rid]]
			replica, _ := leader.Replica(rid)
			timestamp := rangeModelTimestamp(history[rid], rng)
			assertMVCCModelGet(t, replica, history[rid], key, timestamp)
			stats.gets++
		case event%211 == 0:
			replica, _ := cluster.nodes[leaders[rid]].Replica(rid)
			timestamp := rangeModelTimestamp(history[rid], rng)
			assertMVCCModelScan(t, replica, history[rid], timestamp)
			stats.scans++
		case event%307 == 0:
			replica, _ := cluster.nodes[leaders[rid]].Replica(rid)
			snapshot, err := replica.NewSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			assertMVCCModelScan(t, replica, history[rid], snapshot.Timestamp())
			if err = snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			stats.snapshots++
		case event > 0 && event%1000 == 0:
			current := leaders[rid]
			for _, member := range descriptor.Replicas {
				if member.NodeID != current {
					leaders[rid] = member.NodeID
					break
				}
			}
			cluster.elect(rid, leaders[rid])
			stats.leaders++
		case event > 0 && event%2500 == 0:
			for _, member := range descriptor.Replicas {
				if member.NodeID != leaders[rid] {
					if err := cluster.nodes[member.NodeID].RestartRange(rid); err != nil {
						t.Fatal(err)
					}
					cluster.scheduler.Refresh()
					stats.rangeRestarts++
					break
				}
			}
			driveMVCCLeader(t, cluster, rid, leaders[rid])
		case event > 0 && event%333 == 0:
			for _, member := range descriptor.Replicas {
				if member.NodeID != leaders[rid] {
					cluster.transport.SetRangeLink(rid, leaders[rid], member.NodeID, true)
					cluster.transport.SetRangeLink(rid, leaders[rid], member.NodeID, false)
					stats.partitions++
					break
				}
			}
		case event > 0 && event%500 == 0:
			replica, _ := cluster.nodes[leaders[rid]].Replica(rid)
			if err := replica.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			stats.flushes++
		case event > 0 && event%1500 == 1:
			replica, _ := cluster.nodes[leaders[rid]].Replica(rid)
			if err := replica.Compact(context.Background()); err == nil {
				stats.compactions++
			}
		case event > 0 && event%4000 == 2:
			var restartNode raft.NodeID
			for _, candidate := range cluster.bootstrap.Nodes {
				isLeader := false
				for _, leader := range leaders {
					if candidate == leader {
						isLeader = true
						break
					}
				}
				if !isLeader {
					restartNode = candidate
					break
				}
			}
			cluster.scheduler.RemoveNode(restartNode)
			if err := cluster.nodes[restartNode].Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			delete(cluster.nodes, restartNode)
			stats.nodeCrashes++
			node, err := OpenNode(NodeOptions{NodeID: restartNode, Directory: cluster.nodeRoot(restartNode), MVCC: true, Clock: cluster.clocks[restartNode], MemTableBytes: 256 + uint64(restartNode)*128})
			if err != nil {
				t.Fatal(err)
			}
			cluster.nodes[restartNode] = node
			if err = cluster.scheduler.AddNode(node); err != nil {
				t.Fatal(err)
			}
			stats.nodeRestarts++
			for _, assigned := range cluster.bootstrap.Ranges {
				if _, ok := assigned.ReplicaOn(restartNode); ok {
					driveMVCCLeader(t, cluster, assigned.RangeID, leaders[assigned.RangeID])
				}
			}
		default:
			replica, _ := cluster.nodes[leaders[rid]].Replica(rid)
			timestamp := rangeModelTimestamp(history[rid], rng)
			assertMVCCModelGet(t, replica, history[rid], key, timestamp)
			stats.gets++
		}
	}
	for _, descriptor := range cluster.bootstrap.Ranges {
		rid := descriptor.RangeID
		driveMVCCLeader(t, cluster, rid, leaders[rid])
		timestamp := rangeModelTimestamp(history[rid], rng)
		var expected [32]byte
		for index, member := range descriptor.Replicas {
			replica, _ := cluster.nodes[member.NodeID].Replica(rid)
			digest, err := replica.DigestAt(context.Background(), timestamp)
			if err != nil {
				t.Fatal(err)
			}
			if index == 0 {
				expected = digest
			} else if digest != expected {
				t.Fatalf("range %d digest mismatch", rid)
			}
		}
	}
	t.Logf("seed=%d nodes=5 ranges=3 events=%d puts=%d deletes=%d get_at=%d scan_at=%d snapshots=%d leader_changes=%d range_partitions=%d range_restarts=%d node_crashes=%d node_restarts=%d flushes=%d compactions=%d digest_mismatches=0 mvcc_invariant_violations=0", seed, events, stats.puts, stats.deletes, stats.gets, stats.scans, stats.snapshots, stats.leaders, stats.partitions, stats.rangeRestarts, stats.nodeCrashes, stats.nodeRestarts, stats.flushes, stats.compactions)
}

func driveMVCCLeader(t testing.TB, c *multiTestCluster, rid RangeID, leader raft.NodeID) {
	t.Helper()
	for i := 0; i < 3; i++ {
		out, err := c.nodes[leader].Tick(rid)
		if err != nil {
			t.Fatal(err)
		}
		c.send(out)
		c.drain(10000)
	}
}
func mvccRangeKey(rid RangeID, index int) []byte {
	switch rid {
	case 10:
		return []byte(fmt.Sprintf("a-%02d", index))
	case 11:
		return []byte(fmt.Sprintf("g-%02d", index))
	default:
		return []byte(fmt.Sprintf("p-%02d", index))
	}
}
func rangeModelTimestamp(history map[string][]mvccModelVersion, rng interface{ IntN(int) int }) mvcc.Timestamp {
	var values []mvcc.Timestamp
	values = append(values, 0)
	for _, versions := range history {
		for _, version := range versions {
			values = append(values, version.timestamp)
		}
	}
	return values[rng.IntN(len(values))]
}
func mvccModelVisible(versions []mvccModelVersion, timestamp mvcc.Timestamp) (mvccModelVersion, bool) {
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].timestamp <= timestamp {
			return versions[i], true
		}
	}
	return mvccModelVersion{}, false
}
func assertMVCCModelGet(t testing.TB, replica *replicatedrange.Replica, history map[string][]mvccModelVersion, key []byte, timestamp mvcc.Timestamp) {
	t.Helper()
	want, found := mvccModelVisible(history[string(key)], timestamp)
	got, err := replica.GetAt(context.Background(), key, timestamp)
	if !found || want.deleted {
		if !errors.Is(err, engine.ErrNotFound) {
			t.Fatalf("GetAt %x/%d=%q %v", key, timestamp, got, err)
		}
		return
	}
	if err != nil || !bytes.Equal(got, want.value) {
		t.Fatalf("GetAt %x/%d=%q %v want=%q", key, timestamp, got, err, want.value)
	}
}
func assertMVCCModelScan(t testing.TB, replica *replicatedrange.Replica, history map[string][]mvccModelVersion, timestamp mvcc.Timestamp) {
	t.Helper()
	var want []engine.KV
	for key, versions := range history {
		value, ok := mvccModelVisible(versions, timestamp)
		if ok && !value.deleted {
			want = append(want, engine.KV{Key: []byte(key), Value: value.value})
		}
	}
	slices.SortFunc(want, func(a, b engine.KV) int { return bytes.Compare(a.Key, b.Key) })
	got, err := replica.ScanAt(context.Background(), nil, nil, timestamp)
	if err != nil || len(got) != len(want) {
		t.Fatalf("ScanAt %d=%d/%d %v", timestamp, len(got), len(want), err)
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("ScanAt mismatch")
		}
	}
}
