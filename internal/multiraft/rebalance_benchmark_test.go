package multiraft

import (
	"fmt"
	"testing"

	"github.com/rivetdb/rivetdb/internal/raft"
)

func BenchmarkRebalancePlanner(b *testing.B) {
	for _, ranges := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("ranges-%d", ranges), func(b *testing.B) {
			policy := moveOnlyPolicy()
			snapshot := benchmarkRebalanceSnapshot(ranges)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkRebalanceSnapshot(rangeCount int) RebalanceClusterSnapshot {
	snapshot := moveSnapshot(80_000, 20_000)
	snapshot.Ranges = nil
	snapshot.Nodes[0].HostedReplicas, snapshot.Nodes[1].HostedReplicas = 0, 0
	for index := range rangeCount {
		rangeID := RangeID(index + 1)
		snapshot.Ranges = append(snapshot.Ranges, RebalanceRangeMetric{RangeID: rangeID, Generation: 1, Active: true, Samples: 3,
			LogicalBytes: 1 << 20, PhysicalBytes: 2 << 20, WriteRate: uint64(index%100 + 1),
			Replicas: []RebalanceReplicaMetric{{RangeID: rangeID, ReplicaID: ReplicaID(index*2 + 1), NodeID: raft.NodeID(1), Role: "VOTER", Healthy: true},
				{RangeID: rangeID, ReplicaID: ReplicaID(index*2 + 2), NodeID: raft.NodeID(3), Role: "VOTER", Healthy: true}}})
		snapshot.Nodes[0].HostedReplicas++
	}
	return snapshot
}
