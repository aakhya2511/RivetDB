package multiraft

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
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

func BenchmarkRebalanceValidation(b *testing.B) {
	for _, ranges := range []int{100, 1000} {
		b.Run(fmt.Sprintf("ranges-%d", ranges), func(b *testing.B) {
			policy := moveOnlyPolicy()
			snapshot := benchmarkRebalanceSnapshot(ranges)
			plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := ValidateRebalancePlan(plan, snapshot, policy); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRebalanceCollect(b *testing.B) {
	for _, ranges := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("ranges-%d", ranges), func(b *testing.B) {
			catalog := benchmarkCatalog(b, ranges)
			bootstrap := catalog.Snapshot()
			cluster := &multiTestCluster{t: b, root: testutil.BenchmarkDir(b), bootstrap: bootstrap,
				nodes: make(map[raft.NodeID]*Node), mvcc: true, clocks: make(map[raft.NodeID]clock.Clock)}
			for _, nodeID := range bootstrap.Nodes {
				cluster.clocks[nodeID] = clock.NewMock()
			}
			cluster.openRuntime(true)
			defer cluster.close()
			meta, err := OpenMetaRange(MetaRangeOptions{Nodes: bootstrap.Nodes, Directory: filepath.Join(cluster.root, "metadata"), Bootstrap: catalog})
			if err != nil {
				b.Fatal(err)
			}
			collector, err := NewRebalanceCollector(meta, cluster.router, clock.NewMock(), 500_000, 1)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := collector.Collect(context.Background()); err != nil {
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
