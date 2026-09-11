package multiraft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
)

func TestMultiRaftMVCCRoutingHistoricalIsolationAndSkew(t *testing.T) {
	cluster := newMVCCMultiTestCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	cluster.elect(12, 5)
	keys := map[RangeID][]byte{10: []byte("a"), 11: []byte("g"), 12: []byte("p")}
	leaders := map[RangeID]raft.NodeID{10: 1, 11: 3, 12: 5}
	timestamps := make(map[RangeID]mvcc.Timestamp)
	for _, rangeID := range []RangeID{10, 11, 12} {
		timestamp, err := cluster.router.PutMVCC(context.Background(), keys[rangeID], []byte{byte(rangeID)})
		if err != nil {
			t.Fatalf("range %d: %v", rangeID, err)
		}
		timestamps[rangeID] = timestamp
		for round := 0; round < 3; round++ {
			out, tickErr := cluster.nodes[leaders[rangeID]].Tick(rangeID)
			if tickErr != nil {
				t.Fatal(tickErr)
			}
			cluster.send(out)
			cluster.drain(10_000)
		}
	}
	for _, descriptor := range cluster.bootstrap.Ranges {
		var expected [32]byte
		for offset, member := range descriptor.Replicas {
			replica, err := cluster.nodes[member.NodeID].Replica(descriptor.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := replica.DigestAt(context.Background(), timestamps[descriptor.RangeID])
			if err != nil {
				t.Fatal(err)
			}
			if offset == 0 {
				expected = digest
			} else if digest != expected {
				t.Fatalf("range %d historical digest mismatch", descriptor.RangeID)
			}
		}
	}
	cluster.elect(10, 2)
	newTimestamp, err := cluster.router.PutMVCC(context.Background(), []byte("b"), []byte("new-leader"))
	if err != nil {
		t.Fatal(err)
	}
	if newTimestamp <= timestamps[10] {
		t.Fatalf("skewed leader regressed %d <= %d", newTimestamp, timestamps[10])
	}
}

func TestMultiRaftMVCCTimestampFailureIsRangeLocal(t *testing.T) {
	bootstrap := threeRangeBootstrap()
	cluster := &multiTestCluster{
		t: t, root: t.TempDir(), bootstrap: bootstrap, nodes: make(map[raft.NodeID]*Node), mvcc: true,
		clocks: make(map[raft.NodeID]clock.Clock),
	}
	for _, nodeID := range bootstrap.Nodes {
		physicalMillis := int64(10_000 + nodeID)
		if nodeID == 1 {
			physicalMillis = -1
		}
		cluster.clocks[nodeID] = clock.NewMockAt(time.UnixMilli(physicalMillis))
	}
	cluster.openRuntime(true)
	t.Cleanup(func() { cluster.close() })
	cluster.elect(10, 1)
	cluster.elect(11, 3)

	if _, err := cluster.router.PutMVCC(context.Background(), []byte("a"), []byte("blocked")); !errors.Is(err, mvcc.ErrTimestampExhausted) {
		t.Fatalf("range 10 timestamp failure=%v", err)
	}
	timestamp, err := cluster.router.PutMVCC(context.Background(), []byte("g"), []byte("available"))
	if err != nil || timestamp == 0 {
		t.Fatalf("range 11 after range 10 failure timestamp=%d err=%v", timestamp, err)
	}
}
