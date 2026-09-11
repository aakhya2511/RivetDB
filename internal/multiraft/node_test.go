package multiraft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
)

type multiTestCluster struct {
	t         testing.TB
	root      string
	bootstrap Bootstrap
	nodes     map[raft.NodeID]*Node
	transport *Transport
	scheduler *Scheduler
	router    *Router
	mvcc      bool
	clocks    map[raft.NodeID]clock.Clock
}

func newMultiTestCluster(t testing.TB) *multiTestCluster {
	return newMultiTestClusterAt(t, threeRangeBootstrap(), t.TempDir())
}

func newMultiTestClusterWithBootstrap(t testing.TB, bootstrap Bootstrap) *multiTestCluster {
	return newMultiTestClusterAt(t, bootstrap, t.TempDir())
}

func newMultiTestClusterAt(t testing.TB, bootstrap Bootstrap, root string) *multiTestCluster {
	t.Helper()
	cluster := &multiTestCluster{t: t, root: root, bootstrap: bootstrap, nodes: make(map[raft.NodeID]*Node)}
	cluster.openRuntime(true)
	t.Cleanup(func() { cluster.close() })
	return cluster
}

func newMVCCMultiTestCluster(t testing.TB) *multiTestCluster {
	t.Helper()
	bootstrap := threeRangeBootstrap()
	cluster := &multiTestCluster{t: t, root: t.TempDir(), bootstrap: bootstrap, nodes: make(map[raft.NodeID]*Node), mvcc: true, clocks: make(map[raft.NodeID]clock.Clock)}
	for _, nodeID := range bootstrap.Nodes {
		cluster.clocks[nodeID] = clock.NewMockAt(time.UnixMilli(10_000 + int64(nodeID)*1_000))
	}
	cluster.openRuntime(true)
	t.Cleanup(func() { cluster.close() })
	return cluster
}

func (c *multiTestCluster) openRuntime(withBootstrap bool) {
	c.t.Helper()
	transport, err := NewTransport(100_000)
	if err != nil {
		c.t.Fatal(err)
	}
	scheduler, err := NewScheduler(transport)
	if err != nil {
		c.t.Fatal(err)
	}
	c.transport, c.scheduler = transport, scheduler
	for _, nodeID := range c.bootstrap.Nodes {
		var bootstrap *Bootstrap
		if withBootstrap {
			bootstrap = &c.bootstrap
		}
		node, openErr := OpenNode(NodeOptions{NodeID: nodeID, Directory: c.nodeRoot(nodeID), Bootstrap: bootstrap, MemTableBytes: 256 + uint64(nodeID)*128,
			MVCC: c.mvcc, Clock: c.clocks[nodeID]})
		if openErr != nil {
			c.t.Fatalf("open node %d: %v", nodeID, openErr)
		}
		c.nodes[nodeID] = node
		if addErr := scheduler.AddNode(node); addErr != nil {
			c.t.Fatal(addErr)
		}
	}
	if validateErr := ValidateNodeCatalogs(nodeSlice(c.nodes)...); validateErr != nil {
		c.t.Fatal(validateErr)
	}
	c.router, err = NewRouter(RouterOptions{Catalog: c.nodes[1].Catalog(), Scheduler: scheduler, Transport: transport, MaxWork: 50_000})
	if err != nil {
		c.t.Fatal(err)
	}
}

func (c *multiTestCluster) nodeRoot(nodeID raft.NodeID) string {
	return filepath.Join(c.root, fmt.Sprintf("node-%d", nodeID))
}

func (c *multiTestCluster) elect(rangeID RangeID, nodeID raft.NodeID) {
	c.t.Helper()
	for round := 0; round < 30; round++ {
		outbound, err := c.nodes[nodeID].Tick(rangeID)
		if err != nil {
			c.t.Fatalf("tick node=%d range=%d: %v", nodeID, rangeID, err)
		}
		c.send(outbound)
		c.drain(10_000)
		status := rangeStatus(c.t, c.nodes[nodeID], rangeID)
		if status.Raft.Role == raft.Leader {
			c.router.RecordLeader(rangeID, nodeID)
			return
		}
	}
	c.t.Fatalf("node=%d did not lead range=%d", nodeID, rangeID)
}

func (c *multiTestCluster) send(envelopes []Envelope) {
	c.t.Helper()
	for _, envelope := range envelopes {
		if err := c.transport.Send(envelope); err != nil {
			c.t.Fatal(err)
		}
	}
}

func (c *multiTestCluster) drain(limit int) {
	c.t.Helper()
	for delivered := 0; c.transport.Pending() != 0 && delivered < limit; delivered++ {
		if _, err := c.scheduler.DeliverNext(); err != nil {
			c.t.Fatal(err)
		}
	}
	if c.transport.Pending() != 0 {
		c.t.Fatalf("message queue did not drain: %d", c.transport.Pending())
	}
}

func (c *multiTestCluster) close() {
	if c.scheduler != nil {
		c.scheduler.Stop()
	}
	if c.transport != nil {
		c.transport.Stop()
	}
	for _, node := range c.nodes {
		_ = node.Close(context.Background())
	}
	c.nodes = make(map[raft.NodeID]*Node)
}

func rangeStatus(t testing.TB, node *Node, rangeID RangeID) replicatedrange.Status {
	t.Helper()
	for _, status := range node.Status().Ranges {
		if status.RangeID == rangeID {
			return status
		}
	}
	t.Fatalf("node=%d has no range=%d", node.ID(), rangeID)
	return replicatedrange.Status{}
}

func nodeSlice(nodes map[raft.NodeID]*Node) []*Node {
	result := make([]*Node, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].id < result[right].id })
	return result
}

func TestNodeHostsIndependentRangesAndRoutesMutations(t *testing.T) {
	cluster := newMultiTestCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	cluster.elect(12, 5)
	for index := 0; index < 90; index++ {
		key := []byte(fmt.Sprintf("a-%03d", index))
		if index%3 == 1 {
			key = []byte(fmt.Sprintf("g-%03d", index))
		}
		if index%3 == 2 {
			key = []byte(fmt.Sprintf("p-%03d", index))
		}
		if err := cluster.router.Put(context.Background(), key, []byte(fmt.Sprintf("value-%d", index))); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	cluster.drain(100_000)
	for _, rangeID := range []RangeID{10, 11, 12} {
		descriptor, _ := cluster.nodes[1].Catalog().LookupByID(rangeID)
		var digest [32]byte
		var applied []uint64
		var tables []uint64
		for index, member := range descriptor.Replicas {
			replica, err := cluster.nodes[member.NodeID].Replica(rangeID)
			if err != nil {
				t.Fatal(err)
			}
			values, err := replica.LocalScan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range values {
				if !descriptor.Contains(value.Key) {
					t.Fatalf("range=%d contains key=%x", rangeID, value.Key)
				}
			}
			current, err := replica.Digest(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if index == 0 {
				digest = current
			} else if current != digest {
				t.Fatalf("range=%d digest mismatch", rangeID)
			}
			status := replica.Status()
			applied = append(applied, status.Raft.LastApplied)
			tables = append(tables, status.LiveTableCount)
		}
		t.Logf("range=%d commands=30 applied=%v live_tables=%v digest_mismatches=0", rangeID, applied, tables)
	}
	if rangeStatus(t, cluster.nodes[1], 10).Raft.Role != raft.Leader ||
		rangeStatus(t, cluster.nodes[3], 11).Raft.Role != raft.Leader ||
		rangeStatus(t, cluster.nodes[5], 12).Raft.Role != raft.Leader {
		t.Fatal("independent leaders changed unexpectedly")
	}
}

func TestWrongRangeGenerationKeyAndMessageRejected(t *testing.T) {
	cluster := newMultiTestCluster(t)
	cluster.elect(10, 1)
	command, _ := replicatedrange.EncodeCommand(replicatedrange.Command{Type: replicatedrange.CommandPut, Key: []byte("g"), Value: []byte("bad")})
	if _, _, err := cluster.nodes[1].Propose(context.Background(), Route{RangeID: 10, Generation: 1, Key: []byte("g")}, command); !errors.Is(err, ErrWrongRangeKey) {
		t.Fatalf("wrong key=%v", err)
	}
	good, _ := replicatedrange.EncodeCommand(replicatedrange.Command{Type: replicatedrange.CommandPut, Key: []byte("a"), Value: []byte("v")})
	if _, _, err := cluster.nodes[1].Propose(context.Background(), Route{RangeID: 10, Generation: 99, Key: []byte("a")}, good); !errors.Is(err, ErrStaleRange) {
		t.Fatalf("stale generation=%v", err)
	}
	outbound, err := cluster.nodes[1].Tick(10)
	if err != nil || len(outbound) == 0 {
		t.Fatalf("heartbeat=%d err=%v", len(outbound), err)
	}
	wrong := outbound[0]
	wrong.RangeID = 11
	if _, err := cluster.nodes[wrong.Message.To].Step(wrong); !errors.Is(err, ErrWrongRangeMessage) {
		t.Fatalf("wrong range message=%v", err)
	}
	unknown := outbound[0]
	unknown.RangeID, unknown.MessageRangeID = 99, 99
	if _, err := cluster.nodes[unknown.Message.To].Step(unknown); !errors.Is(err, ErrUnknownRange) {
		t.Fatalf("unknown range=%v", err)
	}
}

func TestRangeSpecificQuorumLossDoesNotBlockOtherRanges(t *testing.T) {
	cluster := newMultiTestCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	for _, peer := range []raft.NodeID{2, 3} {
		cluster.transport.SetRangeLink(10, 1, peer, true)
		cluster.transport.SetRangeLink(10, peer, 1, true)
	}
	encoded, _ := replicatedrange.EncodeCommand(replicatedrange.Command{Type: replicatedrange.CommandPut, Key: []byte("blocked"), Value: []byte("x")})
	pending, outbound, err := cluster.nodes[1].Propose(context.Background(), Route{RangeID: 10, Generation: 1, Key: []byte("blocked")}, encoded)
	if err != nil {
		t.Fatal(err)
	}
	cluster.send(outbound)
	if runErr := cluster.scheduler.Run(300); runErr != nil {
		t.Fatal(runErr)
	}
	if done, _ := pending.poll(); done {
		t.Fatal("minority range proposal completed")
	}
	if putErr := cluster.router.Put(context.Background(), []byte("healthy"), []byte("committed")); putErr != nil {
		t.Fatalf("unrelated range failed: %v", putErr)
	}
	value, err := cluster.nodes[3].registry.ranges[11].LocalGet(context.Background(), []byte("healthy"))
	if err != nil || string(value) != "committed" {
		t.Fatalf("healthy value=%q err=%v", value, err)
	}
}

func TestNodeRestartRecoversMultipleRanges(t *testing.T) {
	cluster := newMultiTestCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	cluster.elect(12, 5)
	if err := cluster.router.Put(context.Background(), []byte("before-g"), []byte("left")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.router.Put(context.Background(), []byte("z"), []byte("right")); err != nil {
		t.Fatal(err)
	}
	cluster.scheduler.RemoveNode(1)
	if err := cluster.nodes[1].Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	delete(cluster.nodes, 1)
	if err := cluster.router.Put(context.Background(), []byte("healthy-after"), []byte("alive")); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNode(NodeOptions{NodeID: 1, Directory: cluster.nodeRoot(1), MemTableBytes: 256 + 128})
	if err != nil {
		t.Fatal(err)
	}
	cluster.nodes[1] = reopened
	if err := cluster.scheduler.AddNode(reopened); err != nil {
		t.Fatal(err)
	}
	cluster.drain(10_000)
	if len(reopened.Status().Ranges) != 2 {
		t.Fatalf("reopened ranges=%d", len(reopened.Status().Ranges))
	}
}

func TestNodeCrashHasDifferentPerRangeQuorumEffects(t *testing.T) {
	bootstrap := Bootstrap{Generation: 1, Nodes: []raft.NodeID{1, 2, 3, 4, 5}, ReplicationFactor: 3, Ranges: []RangeDescriptor{
		{RangeID: 10, Generation: 1, StartKey: KeyBound{Unbounded: true}, EndKey: KeyBound{Key: []byte("m")}, Replicas: replicas(1, 2, 3)},
		{RangeID: 11, Generation: 1, StartKey: KeyBound{Key: []byte("m")}, EndKey: KeyBound{Unbounded: true}, Replicas: replicas(1, 4, 5)},
	}}
	cluster := newMultiTestClusterWithBootstrap(t, bootstrap)
	cluster.elect(10, 1)
	cluster.elect(11, 1)
	cluster.scheduler.RemoveNode(1)
	if err := cluster.nodes[1].Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	delete(cluster.nodes, 1)
	cluster.elect(10, 2)
	cluster.elect(11, 4)
	if err := cluster.router.Put(context.Background(), []byte("left"), []byte("after-node1")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.router.Put(context.Background(), []byte("right"), []byte("after-node1")); err != nil {
		t.Fatal(err)
	}
	cluster.scheduler.RemoveNode(2)
	if err := cluster.nodes[2].Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	delete(cluster.nodes, 2)
	for range 20 {
		outbound, err := cluster.nodes[3].Tick(10)
		if err != nil {
			t.Fatal(err)
		}
		cluster.send(outbound)
		cluster.drain(10_000)
	}
	if rangeStatus(t, cluster.nodes[3], 10).Raft.Role == raft.Leader {
		t.Fatal("range 10 elected without quorum")
	}
	if err := cluster.router.Put(context.Background(), []byte("still-right"), []byte("range11-healthy")); err != nil {
		t.Fatalf("range 11 lost unrelated quorum: %v", err)
	}
}

func TestLeaderHintsAreIndependentAndRecoverFromOneStaleHint(t *testing.T) {
	cluster := newMultiTestCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	cluster.elect(12, 5)
	cluster.elect(10, 2)
	outbound, err := cluster.nodes[2].Tick(10)
	if err != nil {
		t.Fatal(err)
	}
	cluster.send(outbound)
	cluster.drain(10_000)
	if cluster.router.LeaderHint(10) != 2 || cluster.router.LeaderHint(11) != 3 || cluster.router.LeaderHint(12) != 5 {
		t.Fatalf("hints r10=%d r11=%d r12=%d", cluster.router.LeaderHint(10), cluster.router.LeaderHint(11), cluster.router.LeaderHint(12))
	}
	cluster.router.RecordLeader(10, 1)
	if err := cluster.router.Put(context.Background(), []byte("f-leader-change"), []byte("ok")); err != nil {
		t.Fatalf("bounded stale-hint retry: %v", err)
	}
	if cluster.router.LeaderHint(10) != 2 || cluster.router.LeaderHint(11) != 3 {
		t.Fatalf("post-retry hints r10=%d r11=%d", cluster.router.LeaderHint(10), cluster.router.LeaderHint(11))
	}
}

func TestRealFilesystemMultiRangeRestartCampaign(t *testing.T) {
	cluster := newMultiTestCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	cluster.elect(12, 5)
	compactions := 0
	for cycle := 0; cycle < 12; cycle++ {
		keys := [][]byte{[]byte(fmt.Sprintf("a-cycle-%02d", cycle)), []byte(fmt.Sprintf("g-cycle-%02d", cycle)), []byte(fmt.Sprintf("p-cycle-%02d", cycle))}
		for _, key := range keys {
			if err := cluster.router.Put(context.Background(), key, []byte("durable")); err != nil {
				t.Fatalf("cycle=%d key=%q: %v", cycle, key, err)
			}
		}
		cluster.drain(100_000)
		for _, rangeID := range []RangeID{10, 11} {
			replica, err := cluster.nodes[2].Replica(rangeID)
			if err != nil {
				t.Fatal(err)
			}
			if err := replica.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := replica.Compact(context.Background()); err == nil {
				compactions++
			} else if !errors.Is(err, compaction.ErrNoCompaction) {
				t.Fatal(err)
			}
		}
		cluster.scheduler.RemoveNode(2)
		if err := cluster.nodes[2].Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		reopened, err := OpenNode(NodeOptions{NodeID: 2, Directory: cluster.nodeRoot(2), MemTableBytes: 256 + 2*128})
		if err != nil {
			t.Fatal(err)
		}
		cluster.nodes[2] = reopened
		if err := cluster.scheduler.AddNode(reopened); err != nil {
			t.Fatal(err)
		}
		for _, leader := range []struct {
			rangeID RangeID
			nodeID  raft.NodeID
		}{{10, 1}, {11, 3}, {12, 5}} {
			outbound, tickErr := cluster.nodes[leader.nodeID].Tick(leader.rangeID)
			if tickErr != nil {
				t.Fatal(tickErr)
			}
			cluster.send(outbound)
		}
		cluster.drain(100_000)
	}
	for _, descriptor := range cluster.bootstrap.Ranges {
		var digest [32]byte
		for index, member := range descriptor.Replicas {
			replica, err := cluster.nodes[member.NodeID].Replica(descriptor.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			current, err := replica.Digest(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if index == 0 {
				digest = current
			} else if current != digest {
				t.Fatalf("range=%d digest mismatch after campaign", descriptor.RangeID)
			}
		}
	}
	t.Logf("cycles=12 routed_mutations=36 node_restarts=12 flushes=24 successful_compactions=%d digest_mismatches=0", compactions)
}

func TestFullClusterRestartRecoversCatalogAndRangeState(t *testing.T) {
	cluster := newMultiTestCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	cluster.elect(12, 5)
	for index := 0; index < 30; index++ {
		key := []byte(fmt.Sprintf("a%02d", index))
		if index%3 == 1 {
			key = []byte(fmt.Sprintf("g%02d", index))
		}
		if index%3 == 2 {
			key = []byte(fmt.Sprintf("p%02d", index))
		}
		if err := cluster.router.Put(context.Background(), key, []byte("persisted")); err != nil {
			t.Fatal(err)
		}
	}
	cluster.close()
	cluster.openRuntime(false)
	cluster.elect(10, 2)
	cluster.elect(11, 4)
	cluster.elect(12, 3)
	for _, descriptor := range cluster.bootstrap.Ranges {
		var digest [32]byte
		var applied []uint64
		for index, member := range descriptor.Replicas {
			replica, err := cluster.nodes[member.NodeID].Replica(descriptor.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			current, err := replica.Digest(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if index == 0 {
				digest = current
			} else if current != digest {
				t.Fatalf("range=%d restart digest mismatch", descriptor.RangeID)
			}
			applied = append(applied, replica.Status().Raft.LastApplied)
		}
		t.Logf("range=%d replicas=%d precrash_commands=10 applied=%v digest_mismatches=0", descriptor.RangeID, len(descriptor.Replicas), applied)
	}
}

func TestCatalogCorruptionMissingRangeAndOrphanAreExplicit(t *testing.T) {
	root := t.TempDir()
	bootstrap := threeRangeBootstrap()
	node, err := OpenNode(NodeOptions{NodeID: 1, Directory: root, Bootstrap: &bootstrap})
	if err != nil {
		t.Fatal(err)
	}
	if closeErr := node.Close(context.Background()); closeErr != nil {
		t.Fatal(closeErr)
	}
	if mkdirErr := os.MkdirAll(filepath.Join(root, "ranges", "99"), 0o750); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	missing := filepath.Join(root, "ranges", "10", "raft")
	if renameErr := os.Rename(missing, missing+".missing"); renameErr != nil {
		t.Fatal(renameErr)
	}
	reopened, err := OpenNode(NodeOptions{NodeID: 1, Directory: root})
	if err != nil {
		t.Fatal(err)
	}
	status := reopened.Status()
	if len(status.Failures) != 1 || status.Failures[0].RangeID != 10 || !errors.Is(status.Failures[0].Err, ErrMissingRange) {
		t.Fatalf("failures=%+v", status.Failures)
	}
	if len(status.Orphans) != 1 || status.Orphans[0] != "99" {
		t.Fatalf("orphans=%v", status.Orphans)
	}
	if _, tickErr := reopened.Tick(12); tickErr != nil {
		t.Fatalf("healthy range stopped with failed range: %v", tickErr)
	}
	if closeErr := reopened.Close(context.Background()); closeErr != nil {
		t.Fatal(closeErr)
	}
	path := filepath.Join(root, "cluster", catalogFilename)
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)/2] ^= 1
	if writeErr := os.WriteFile(path, encoded, 0o640); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, corruptErr := OpenNode(NodeOptions{NodeID: 1, Directory: root}); !errors.Is(corruptErr, ErrCorruptCatalog) {
		t.Fatalf("corrupt catalog=%v", corruptErr)
	}
}

func TestCorruptBootstrapCompletionMarkerFailsStartup(t *testing.T) {
	root := t.TempDir()
	bootstrap := threeRangeBootstrap()
	node, err := OpenNode(NodeOptions{NodeID: 1, Directory: root, Bootstrap: &bootstrap})
	if err != nil {
		t.Fatal(err)
	}
	if closeErr := node.Close(context.Background()); closeErr != nil {
		t.Fatal(closeErr)
	}
	marker := filepath.Join(root, "cluster", bootstrapCompleteFilename)
	encoded, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 1
	if writeErr := os.WriteFile(marker, encoded, 0o640); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, openErr := OpenNode(NodeOptions{NodeID: 1, Directory: root}); !errors.Is(openErr, ErrCorruptCatalog) {
		t.Fatalf("corrupt completion marker=%v", openErr)
	}
}

func TestSharedSchedulerManyGroupsHasNoRuntimeGoroutines(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()
	var memoryBefore runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	const count = 100
	ranges := make([]RangeDescriptor, count)
	for index := range ranges {
		start, end := KeyBound{Key: []byte{byte(index)}}, KeyBound{Key: []byte{byte(index + 1)}}
		if index == 0 {
			start = KeyBound{Unbounded: true}
		}
		if index == count-1 {
			end = KeyBound{Unbounded: true}
		}
		ranges[index] = RangeDescriptor{RangeID: RangeID(index + 1), Generation: 1, StartKey: start, EndKey: end,
			Replicas: []ReplicaDescriptor{{ReplicaID: ReplicaID(index + 1), NodeID: 1}}}
	}
	bootstrap := Bootstrap{Generation: 1, Nodes: []raft.NodeID{1}, ReplicationFactor: 1, Ranges: ranges}
	node, err := OpenNode(NodeOptions{NodeID: 1, Directory: t.TempDir(), Bootstrap: &bootstrap, MaxHostedRanges: count})
	if err != nil {
		t.Fatal(err)
	}
	transport, _ := NewTransport(10_000)
	scheduler, _ := NewScheduler(transport)
	if err := scheduler.AddNode(node); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Run(count * 10); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	after := runtime.NumGoroutine()
	var memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryAfter)
	if after-before > count+10 {
		t.Fatalf("goroutines grew from %d to %d", before, after)
	}
	stats := scheduler.Stats()
	if stats.Ticks != count*10 {
		t.Fatalf("ticks=%d", stats.Ticks)
	}
	transportStats := transport.Stats()
	t.Logf("groups=%d goroutines_before=%d goroutines_hosted=%d scheduler_ticks=%d messages_sent=%d heap_delta_bytes=%d", count, before, after, stats.Ticks, transportStats.Sent, int64(memoryAfter.HeapAlloc)-int64(memoryBefore.HeapAlloc))
	scheduler.Stop()
	transport.Stop()
	if err := node.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
