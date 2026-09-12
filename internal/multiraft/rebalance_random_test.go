package multiraft

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestRandomizedRebalancePlannerModel(t *testing.T) {
	for _, seed := range []int64{9001, 9002, testutil.Seed(t)} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) { runRebalancePlannerModel(t, seed, 10_000) })
	}
}

func TestRandomizedRebalancePlannerHeavy(t *testing.T) {
	if os.Getenv("RIVETDB_REBALANCE_STRESS") == "" {
		t.Skip("set RIVETDB_REBALANCE_STRESS=1 for 100k planner events")
	}
	runRebalancePlannerModel(t, testutil.Seed(t), 100_000)
}

func runRebalancePlannerModel(t testing.TB, seed int64, events int) {
	t.Helper()
	rng := testutil.RandFromSeed(seed)
	policy := moveOnlyPolicy()
	for event := 0; event < events; event++ {
		nodeCount := 3 + rng.IntN(6)
		rangeCount := 1 + rng.IntN(24)
		snapshot := RebalanceClusterSnapshot{SnapshotTime: clock.Epoch.Add(time.Duration(event) * time.Second), CatalogGeneration: uint64(event + 1), PolicyVersion: policy.Version, ControllerEpoch: 1}
		for node := 1; node <= nodeCount; node++ {
			snapshot.Nodes = append(snapshot.Nodes, RebalanceNodeMetric{NodeID: raft.NodeID(node), Healthy: true, Available: true, WriteRate: uint64(rng.IntN(10_000))})
		}
		for id := 1; id <= rangeCount; id++ {
			start := rng.IntN(nodeCount)
			metric := RebalanceRangeMetric{RangeID: RangeID(id), Generation: 1, Active: true, Samples: policy.MinSamples,
				LogicalBytes: uint64(1 + rng.IntN(1<<20)), PhysicalBytes: uint64(1 + rng.IntN(2<<20)), WriteRate: uint64(rng.IntN(1000))}
			for replica := 0; replica < min(3, nodeCount); replica++ {
				nodeID := raft.NodeID((start+replica)%nodeCount + 1)
				metric.Replicas = append(metric.Replicas, RebalanceReplicaMetric{RangeID: metric.RangeID, ReplicaID: ReplicaID(id*10 + replica + 1), NodeID: nodeID, Role: "VOTER", Healthy: true})
				for index := range snapshot.Nodes {
					if snapshot.Nodes[index].NodeID == nodeID {
						snapshot.Nodes[index].HostedReplicas++
						snapshot.Nodes[index].LogicalBytes += metric.LogicalBytes
					}
				}
			}
			snapshot.Ranges = append(snapshot.Ranges, metric)
		}
		plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
		if err != nil {
			t.Fatalf("seed=%d event=%d plan: %v", seed, event, err)
		}
		for _, action := range plan.Actions {
			var metric RebalanceRangeMetric
			for _, candidate := range snapshot.Ranges {
				if candidate.RangeID == action.RangeID {
					metric = candidate
					break
				}
			}
			source, sourceExists := replicaOn(metric, action.SourceNodeID)
			_, targetExists := replicaOn(metric, action.TargetNodeID)
			if action.Type == RebalanceMoveReplica && (!sourceExists || !source.Healthy || source.Role != "VOTER" || targetExists || action.SourceNodeID == action.TargetNodeID) {
				t.Fatalf("seed=%d event=%d unsafe action=%+v", seed, event, action)
			}
		}
		for left, right := 0, len(snapshot.Nodes)-1; left < right; left, right = left+1, right-1 {
			snapshot.Nodes[left], snapshot.Nodes[right] = snapshot.Nodes[right], snapshot.Nodes[left]
		}
		for left, right := 0, len(snapshot.Ranges)-1; left < right; left, right = left+1, right-1 {
			snapshot.Ranges[left], snapshot.Ranges[right] = snapshot.Ranges[right], snapshot.Ranges[left]
		}
		reordered, reorderErr := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
		if reorderErr != nil || !reflect.DeepEqual(plan, reordered) {
			t.Fatalf("seed=%d event=%d nondeterministic err=%v", seed, event, reorderErr)
		}
	}
	t.Logf("seed=%d events=%d duplicate_placements=0 failed_targets=0 nondeterministic_plans=0", seed, events)
}

func TestPlannerRejectsFailedFullAndDuplicateTargets(t *testing.T) {
	policy := moveOnlyPolicy()
	policy.MaxReplicaCountPerNode = 2
	snapshot := moveSnapshot(90, 10)
	snapshot.Nodes[1].Healthy = false
	plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 0 {
		t.Fatalf("failed target plan=%+v err=%v", plan, err)
	}
	snapshot.Nodes[1].Healthy, snapshot.Nodes[1].HostedReplicas = true, 2
	plan, err = PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 0 {
		t.Fatalf("full target plan=%+v err=%v", plan, err)
	}
	snapshot.Nodes[1].HostedReplicas = 0
	snapshot.Ranges[0].Replicas = append(snapshot.Ranges[0].Replicas, RebalanceReplicaMetric{RangeID: 10, ReplicaID: 102, NodeID: 2, Role: "VOTER", Healthy: true})
	plan, err = PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 0 {
		t.Fatalf("duplicate target plan=%+v err=%v", plan, err)
	}
}
