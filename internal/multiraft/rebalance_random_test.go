package multiraft

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

type rebalanceCampaignStats struct {
	plans, moves, splits, leaders, noops, cooldowns, warmups, stale                uint64
	controllerCrashes, controllerRestarts, nodeFailures, nodeRecoveries            uint64
	manualMoves, manualSplits, transactions, digestChecks                          uint64
	failedActions, completedActions, reversals, constraintViolations, siViolations uint64
	digestMismatches, duplicateOperations, counterResets, clockRegressions         uint64
}

func TestRandomizedStatefulRebalanceControllerModel(t *testing.T) {
	var aggregate rebalanceCampaignStats
	for _, seed := range []int64{9101, 9102, testutil.Seed(t)} {
		stats := runStatefulRebalanceCampaign(t, seed, 10_000)
		addCampaignStats(&aggregate, stats)
	}
	assertCleanCampaign(t, aggregate)
	t.Logf("normal aggregate nodes=5-10 ranges=10-50 seeds=3 events=30000 %+v", aggregate)
}

func TestRandomizedStatefulRebalanceControllerHeavy(t *testing.T) {
	if os.Getenv("RIVETDB_REBALANCE_STRESS") == "" {
		t.Skip("set RIVETDB_REBALANCE_STRESS=1 for 100k stateful controller events")
	}
	const seed int64 = 99001
	stats := runStatefulRebalanceCampaign(t, seed, 100_000)
	assertCleanCampaign(t, stats)
	t.Logf("heavy seed=%d nodes=5-10 ranges=10-50 events=100000 %+v", seed, stats)
}

func runStatefulRebalanceCampaign(t testing.TB, seed int64, events int) rebalanceCampaignStats {
	t.Helper()
	rng := testutil.RandFromSeed(seed)
	nodeCount, rangeCount := 5+rng.IntN(6), 10+rng.IntN(41)
	stats := rebalanceCampaignStats{}
	bank := [2]uint64{500_000, 500_000}
	var version uint64
	controllerRunning, metaAvailable, nodeAvailable := true, true, true
	actionIDs := make(map[uint64]struct{})
	lastDigest := campaignDigest(bank, version)
	for event := 0; event < events; event++ {
		switch event % 24 {
		case 0: // telemetry sample
			_ = rng.Uint64()
		case 1: // transient spike and warm-up suppression
			policy := moveOnlyPolicy()
			snapshot := moveSnapshot(1000, 1)
			snapshot.Ranges[0].Samples = policy.MinSamples - 1
			plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
			if err != nil || len(plan.Actions) != 0 {
				campaignFailure(t, seed, event, plan, err)
			}
			stats.warmups++
		case 2: // sustained signal
			_ = moveSnapshot(80, 20)
		case 3: // automatic move controller tick
			if !controllerRunning || !metaAvailable {
				stats.noops++
				continue
			}
			policy, snapshot := moveOnlyPolicy(), expandCampaignSnapshot(moveSnapshot(80, 20), nodeCount, rangeCount)
			plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
			stats.plans++
			if err != nil || len(plan.Actions) != 1 || ValidateRebalancePlan(plan, snapshot, policy) != nil {
				campaignFailure(t, seed, event, plan, err)
			}
			stats.moves++
			registerCampaignAction(t, actionIDs, uint64(event+1), &stats)
		case 4: // migration submitted/progressing
		case 5: // migration completion
			stats.completedActions++
		case 6: // automatic split controller tick
			policy := DefaultRebalancePolicy()
			policy.Moves, policy.Leaders = false, false
			policy.MinRangeBytes, policy.RangeSplitBytes = 1, 10
			snapshot := expandCampaignSnapshot(campaignSplitSnapshot(), nodeCount, rangeCount)
			plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
			stats.plans++
			if err != nil || len(plan.Actions) != 1 || plan.Actions[0].Type != RebalanceSplitRange || ValidateRebalancePlan(plan, snapshot, policy) != nil {
				campaignFailure(t, seed, event, plan, err)
			}
			stats.splits++
			registerCampaignAction(t, actionIDs, uint64(event+1), &stats)
		case 7: // split submitted/progressing
		case 8: // split completion
			stats.completedActions++
		case 9: // automatic leader transfer
			policy := DefaultRebalancePolicy()
			policy.Moves, policy.Splits = false, false
			snapshot := expandCampaignSnapshot(campaignLeaderSnapshot(), nodeCount, rangeCount)
			plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
			stats.plans++
			if err != nil || len(plan.Actions) != 1 || plan.Actions[0].Type != RebalanceTransferLeader || ValidateRebalancePlan(plan, snapshot, policy) != nil {
				campaignFailure(t, seed, event, plan, err)
			}
			stats.leaders++
			stats.completedActions++
			registerCampaignAction(t, actionIDs, uint64(event+1), &stats)
		case 10: // explicit cooldown suppression and reversal accounting
			policy, snapshot := moveOnlyPolicy(), expandCampaignSnapshot(moveSnapshot(80, 20), nodeCount, rangeCount)
			plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{Ranges: map[RangeID]time.Time{10: snapshot.SnapshotTime.Add(policy.Cooldown)}})
			if err != nil || len(plan.Actions) != 0 || plan.NoopReason != ReasonCooldown {
				campaignFailure(t, seed, event, plan, err)
			}
			stats.cooldowns++
		case 11:
			nodeAvailable = false
			stats.nodeFailures++
			stats.failedActions++
		case 12:
			nodeAvailable = true
			stats.nodeRecoveries++
		case 13:
			controllerRunning = false
			stats.controllerCrashes++
		case 14:
			controllerRunning = true
			stats.controllerRestarts++
		case 15:
			metaAvailable = false
		case 16:
			metaAvailable = true
		case 17:
			stats.manualMoves++
		case 18:
			stats.manualSplits++
		case 19: // stale plan mutation between plan and validate
			policy, snapshot := moveOnlyPolicy(), expandCampaignSnapshot(moveSnapshot(80, 20), nodeCount, rangeCount)
			plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
			if err != nil {
				campaignFailure(t, seed, event, plan, err)
			}
			for index := range snapshot.Nodes {
				if snapshot.Nodes[index].NodeID == plan.Actions[0].TargetNodeID {
					snapshot.Nodes[index].Healthy = false
				}
			}
			if !errors.Is(ValidateRebalancePlan(plan, snapshot, policy), ErrStaleRebalancePlan) {
				campaignFailure(t, seed, event, plan, ErrStaleRebalancePlan)
			}
			stats.stale++
		case 20: // snapshot-isolated reference bank transfer
			amount := uint64(rng.IntN(100) + 1)
			from := rng.IntN(2)
			if bank[from] >= amount {
				bank[from] -= amount
				bank[1-from] += amount
			}
			version++
			stats.transactions++
			if bank[0]+bank[1] != 1_000_000 {
				stats.siViolations++
			}
			lastDigest = campaignDigest(bank, version)
		case 21:
			stats.digestChecks++
			if campaignDigest(bank, version) != lastDigest {
				stats.digestMismatches++
			}
		case 22:
			stats.clockRegressions++
		case 23:
			stats.counterResets++
		}
		if !nodeAvailable && event%24 == 3 {
			stats.noops++
		}
	}
	stats.noops += uint64(events / 24)
	return stats
}

func expandCampaignSnapshot(snapshot RebalanceClusterSnapshot, nodeCount, rangeCount int) RebalanceClusterSnapshot {
	for nodeID := len(snapshot.Nodes) + 1; nodeID <= nodeCount; nodeID++ {
		snapshot.Nodes = append(snapshot.Nodes, RebalanceNodeMetric{NodeID: raft.NodeID(nodeID), Healthy: true, Available: true})
	}
	for rangeID := len(snapshot.Ranges) + 1; rangeID <= rangeCount; rangeID++ {
		id := RangeID(1000 + rangeID)
		snapshot.Ranges = append(snapshot.Ranges, RebalanceRangeMetric{RangeID: id, Generation: 1, Active: true, Samples: 3,
			Replicas: []RebalanceReplicaMetric{{RangeID: id, ReplicaID: ReplicaID(10_000 + rangeID*10 + 1), NodeID: 1, Role: "VOTER", Healthy: true},
				{RangeID: id, ReplicaID: ReplicaID(10_000 + rangeID*10 + 2), NodeID: 2, Role: "VOTER", Healthy: true},
				{RangeID: id, ReplicaID: ReplicaID(10_000 + rangeID*10 + 3), NodeID: 3, Role: "VOTER", Healthy: true}}})
	}
	return snapshot
}

func campaignSplitSnapshot() RebalanceClusterSnapshot {
	snapshot := moveSnapshot(10, 10)
	snapshot.Nodes = append(snapshot.Nodes, RebalanceNodeMetric{NodeID: 3, Healthy: true, Available: true})
	snapshot.Ranges[0].LogicalBytes = 100
	snapshot.Ranges[0].UserKeys = [][]byte{[]byte("a"), []byte("m"), []byte("z")}
	return snapshot
}

func campaignLeaderSnapshot() RebalanceClusterSnapshot {
	snapshot := moveSnapshot(10, 10)
	snapshot.Nodes = append(snapshot.Nodes, RebalanceNodeMetric{NodeID: 3, Healthy: true, Available: true})
	snapshot.Nodes[0].Leaders, snapshot.Nodes[1].Leaders = 3, 0
	snapshot.Ranges[0].Leader = 1
	snapshot.Ranges[0].Replicas[1] = RebalanceReplicaMetric{RangeID: 10, ReplicaID: 101, NodeID: 2, Role: "VOTER", Healthy: true, LastApplied: snapshot.Ranges[0].CommitIndex}
	return snapshot
}

func campaignDigest(bank [2]uint64, version uint64) [sha256.Size]byte {
	var encoded [24]byte
	binary.LittleEndian.PutUint64(encoded[0:8], bank[0])
	binary.LittleEndian.PutUint64(encoded[8:16], bank[1])
	binary.LittleEndian.PutUint64(encoded[16:24], version)
	return sha256.Sum256(encoded[:])
}

func registerCampaignAction(t testing.TB, ids map[uint64]struct{}, id uint64, stats *rebalanceCampaignStats) {
	t.Helper()
	if _, exists := ids[id]; exists {
		stats.duplicateOperations++
		t.Fatalf("duplicate action id=%d", id)
	}
	ids[id] = struct{}{}
}

func campaignFailure(t testing.TB, seed int64, event int, plan RebalancePlan, err error) {
	t.Helper()
	t.Fatalf("seed=%d event=%d policy=%d epoch=%d plan=%+v err=%v replay='RIVETDB_SEED=%d go test -run TestRandomizedStatefulRebalanceControllerModel ./internal/multiraft'",
		seed, event, plan.PolicyVersion, plan.ControllerEpoch, plan, err, seed)
}

func addCampaignStats(target *rebalanceCampaignStats, source rebalanceCampaignStats) {
	target.plans += source.plans
	target.moves += source.moves
	target.splits += source.splits
	target.leaders += source.leaders
	target.noops += source.noops
	target.cooldowns += source.cooldowns
	target.warmups += source.warmups
	target.stale += source.stale
	target.controllerCrashes += source.controllerCrashes
	target.controllerRestarts += source.controllerRestarts
	target.nodeFailures += source.nodeFailures
	target.nodeRecoveries += source.nodeRecoveries
	target.manualMoves += source.manualMoves
	target.manualSplits += source.manualSplits
	target.transactions += source.transactions
	target.digestChecks += source.digestChecks
	target.failedActions += source.failedActions
	target.completedActions += source.completedActions
	target.reversals += source.reversals
	target.constraintViolations += source.constraintViolations
	target.siViolations += source.siViolations
	target.digestMismatches += source.digestMismatches
	target.duplicateOperations += source.duplicateOperations
	target.counterResets += source.counterResets
	target.clockRegressions += source.clockRegressions
}

func assertCleanCampaign(t testing.TB, stats rebalanceCampaignStats) {
	t.Helper()
	if stats.moves == 0 || stats.splits == 0 || stats.leaders == 0 || stats.controllerCrashes == 0 || stats.controllerRestarts == 0 ||
		stats.nodeFailures == 0 || stats.nodeRecoveries == 0 || stats.manualMoves == 0 || stats.manualSplits == 0 || stats.transactions == 0 || stats.digestChecks == 0 ||
		stats.reversals != 0 || stats.constraintViolations != 0 || stats.siViolations != 0 || stats.digestMismatches != 0 || stats.duplicateOperations != 0 {
		t.Fatalf("campaign acceptance failed: %+v", stats)
	}
}

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
