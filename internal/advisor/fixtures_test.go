package advisor

import (
	"testing"

	"github.com/rivetdb/rivetdb/internal/multiraft"
)

func TestRecommendationQualityFixtures(t *testing.T) {
	base, policy, cooldowns := testInput()
	assert := func(t *testing.T, snapshot multiraft.RebalanceClusterSnapshot, fixturePolicy multiraft.RebalancePolicy, action multiraft.RebalanceActionType, noop multiraft.RebalanceReason) {
		t.Helper()
		plan, err := multiraft.PlanRebalance(snapshot, fixturePolicy, cooldowns)
		if err != nil {
			t.Fatal(err)
		}
		if action != 0 && (len(plan.Actions) == 0 || plan.Actions[0].Type != action) {
			t.Fatalf("actions=%+v want=%v", plan.Actions, action)
		}
		if action == 0 && plan.NoopReason != noop {
			t.Fatalf("noop=%s want=%s", plan.NoopReason, noop)
		}
	}
	t.Run("leader-skew", func(t *testing.T) { assert(t, base.Cluster, policy, multiraft.RebalanceTransferLeader, "") })
	t.Run("hot-range", func(t *testing.T) {
		snapshot := cloneCluster(base.Cluster)
		snapshot.Nodes[0].Leaders, snapshot.Nodes[1].Leaders, snapshot.Nodes[2].Leaders = 1, 1, 1
		snapshot.Ranges[0].LogicalBytes = policy.RangeSplitBytes
		snapshot.Ranges[0].UserKeys = [][]byte{[]byte("a"), []byte("z")}
		snapshot.Ranges[0].StartUnbounded, snapshot.Ranges[0].EndUnbounded = true, true
		assert(t, snapshot, policy, multiraft.RebalanceSplitRange, "")
	})
	t.Run("storage-overload", func(t *testing.T) {
		snapshot := cloneCluster(base.Cluster)
		snapshot.Nodes = append(snapshot.Nodes, multiraft.RebalanceNodeMetric{NodeID: 4, Healthy: true, Available: true})
		local := policy
		local.Leaders, local.Splits = false, false
		assert(t, snapshot, local, multiraft.RebalanceMoveReplica, "")
	})
	for _, name := range []string{"no-eligible-target", "failed-target"} {
		t.Run(name, func(t *testing.T) {
			snapshot := cloneCluster(base.Cluster)
			if name == "failed-target" {
				snapshot.Nodes = append(snapshot.Nodes, multiraft.RebalanceNodeMetric{NodeID: 4, Healthy: false, Available: false})
			}
			local := policy
			local.Leaders, local.Splits = false, false
			assert(t, snapshot, local, 0, multiraft.ReasonNoEligibleTarget)
		})
	}
	t.Run("balanced", func(t *testing.T) {
		snapshot := cloneCluster(base.Cluster)
		for index := range snapshot.Nodes {
			snapshot.Nodes[index].Leaders = 1
			snapshot.Nodes[index].LogicalBytes = 100
			snapshot.Nodes[index].WriteRate = 10
		}
		local := policy
		local.Splits = false
		assert(t, snapshot, local, 0, multiraft.ReasonBalanced)
	})
}

func cloneCluster(source multiraft.RebalanceClusterSnapshot) multiraft.RebalanceClusterSnapshot {
	result := source
	result.Nodes = append([]multiraft.RebalanceNodeMetric(nil), source.Nodes...)
	result.Ranges = append([]multiraft.RebalanceRangeMetric(nil), source.Ranges...)
	for index := range result.Ranges {
		result.Ranges[index].Replicas = append([]multiraft.RebalanceReplicaMetric(nil), source.Ranges[index].Replicas...)
		result.Ranges[index].UserKeys = append([][]byte(nil), source.Ranges[index].UserKeys...)
	}
	return result
}
