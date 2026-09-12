package multiraft

import (
	"bytes"
	"fmt"
	"math/bits"
	"sort"

	"github.com/rivetdb/rivetdb/internal/raft"
)

type rebalanceCandidate struct {
	action RebalanceAction
	order  uint8
}

// PlanRebalance is pure: equal snapshots, policy and cooldown state produce
// byte-for-byte equivalent plans regardless of input slice order.
func PlanRebalance(observed RebalanceClusterSnapshot, policy RebalancePolicy, cooldowns RebalanceCooldowns) (RebalancePlan, error) {
	if err := policy.Validate(); err != nil {
		return RebalancePlan{}, err
	}
	snapshot, err := CanonicalizeRebalanceSnapshot(observed)
	if err != nil {
		return RebalancePlan{}, err
	}
	plan := RebalancePlan{SnapshotGeneration: snapshot.CatalogGeneration, PolicyVersion: policy.Version, ControllerEpoch: snapshot.ControllerEpoch}
	if !policy.Enabled {
		plan.NoopReason = ReasonBalanced
		return plan, nil
	}
	if snapshot.PolicyVersion != policy.Version || len(snapshot.Nodes) == 0 {
		return RebalancePlan{}, ErrStaleRebalancePlan
	}
	nodeScores := scoreNodes(snapshot.Nodes, policy)
	beforePotential := loadPotential(nodeScores)
	var candidates []rebalanceCandidate
	insufficient, unsplittable, inProgress, cooled, noEligibleTarget := false, false, false, false, false
	activeSplits, activeMigrations := uint64(0), uint64(0)
	for _, node := range snapshot.Nodes {
		activeMigrations = saturatingAdd(activeMigrations, node.MigrationsIn)
	}
	for _, metric := range snapshot.Ranges {
		if metric.Splitting {
			activeSplits++
		}
	}
	for _, metric := range snapshot.Ranges {
		if !metric.Active {
			continue
		}
		rateReady := metric.Samples >= policy.MinSamples
		insufficient = insufficient || !rateReady
		if metric.Splitting || metric.Migrating {
			inProgress = true
			continue
		}
		if !cooldownExpired(snapshot, cooldowns, metric.RangeID, 0) {
			cooled = true
			continue
		}
		if policy.Splits && activeSplits < uint64(policy.MaxConcurrentSplitsCluster) && splitNodesHaveCapacity(snapshot.Nodes, metric, policy) && splitWarranted(metric, policy, rateReady) && uint64(len(snapshot.Ranges)) < policy.MaxRanges {
			key, ok := medianSplitKey(metric)
			if ok {
				candidates = append(candidates, rebalanceCandidate{order: 0, action: RebalanceAction{Type: RebalanceSplitRange,
					Reason: splitReason(metric, policy), RangeID: metric.RangeID, RangeGeneration: metric.Generation,
					CatalogGeneration: snapshot.CatalogGeneration, SplitKey: key, EstimatedCost: max(1, metric.LogicalBytes), ExpectedImprovement: rangeLoad(metric, policy),
					Constraints: []string{"interior user-key median", "range operation exclusion", "split concurrency"}}})
			} else {
				unsplittable = true
			}
		}
	}
	if policy.Leaders {
		if candidate, ok := leaderCandidate(snapshot, policy, nodeScores, beforePotential, cooldowns); ok {
			candidates = append(candidates, rebalanceCandidate{order: 1, action: candidate})
		}
	}
	if policy.Moves && activeMigrations < uint64(policy.MaxConcurrentMigrationsCluster) {
		moves := moveCandidates(snapshot, policy, nodeScores, beforePotential, cooldowns)
		noEligibleTarget = len(moves) == 0 && moveOverloaded(snapshot.Nodes, policy, nodeScores, cooldowns)
		candidates = append(candidates, moves...)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidateLess(candidates[i], candidates[j]) })
	usedRanges := make(map[RangeID]struct{})
	usedNodes := make(map[raft.NodeID]struct{})
	selected := uint32(0)
	for _, candidate := range candidates {
		if selected == policy.MaxActionsPerCycle {
			break
		}
		if candidate.action.ExpectedImprovement < policy.MinExpectedImprovement {
			continue
		}
		if _, used := usedRanges[candidate.action.RangeID]; used {
			continue
		}
		if _, used := usedNodes[candidate.action.SourceNodeID]; used && candidate.action.SourceNodeID != 0 {
			continue
		}
		if _, used := usedNodes[candidate.action.TargetNodeID]; used && candidate.action.TargetNodeID != 0 {
			continue
		}
		plan.Actions = append(plan.Actions, candidate.action)
		selected++
		usedRanges[candidate.action.RangeID] = struct{}{}
		if candidate.action.SourceNodeID != 0 {
			usedNodes[candidate.action.SourceNodeID] = struct{}{}
		}
		if candidate.action.TargetNodeID != 0 {
			usedNodes[candidate.action.TargetNodeID] = struct{}{}
		}
	}
	if len(plan.Actions) == 0 {
		switch {
		case inProgress:
			plan.NoopReason = ReasonOperationInProgress
		case insufficient:
			plan.NoopReason = ReasonInsufficientSamples
		case unsplittable:
			plan.NoopReason = ReasonHotUnsplittableKeyspace
		case cooled:
			plan.NoopReason = ReasonCooldown
		case noEligibleTarget:
			plan.NoopReason = ReasonNoEligibleTarget
		default:
			plan.NoopReason = ReasonBalanced
		}
	}
	return plan, nil
}

func splitNodesHaveCapacity(nodes []RebalanceNodeMetric, metric RebalanceRangeMetric, policy RebalancePolicy) bool {
	for _, replica := range metric.Replicas {
		node, exists := findNode(nodes, replica.NodeID)
		if !exists || !node.Healthy || !node.Available || node.SplitsInFlight >= uint64(policy.MaxSplitsPerNode) {
			return false
		}
	}
	return true
}

// ValidateRebalancePlan rejects observations made obsolete before execution.
func ValidateRebalancePlan(plan RebalancePlan, fresh RebalanceClusterSnapshot, policy RebalancePolicy) error {
	if plan.SnapshotGeneration != fresh.CatalogGeneration || plan.PolicyVersion != fresh.PolicyVersion ||
		plan.PolicyVersion != policy.Version || plan.ControllerEpoch != fresh.ControllerEpoch {
		return ErrStaleRebalancePlan
	}
	ranges := make(map[RangeID]RebalanceRangeMetric, len(fresh.Ranges))
	for _, metric := range fresh.Ranges {
		ranges[metric.RangeID] = metric
	}
	for _, action := range plan.Actions {
		metric, exists := ranges[action.RangeID]
		if !exists || metric.Generation != action.RangeGeneration || metric.Migrating || metric.Splitting || !metric.Active {
			return fmt.Errorf("%w: range %d changed", ErrStaleRebalancePlan, action.RangeID)
		}
		switch action.Type {
		case RebalanceMoveReplica:
			source, sourceOK := findNode(fresh.Nodes, action.SourceNodeID)
			target, targetOK := findNode(fresh.Nodes, action.TargetNodeID)
			replica, hosted := replicaOn(metric, action.SourceNodeID)
			if !sourceOK || !targetOK || !hosted || replica.ReplicaID != action.SourceReplicaID ||
				!replica.Healthy || replica.Role != "VOTER" || !eligibleMoveTarget(metric, source, target, policy) {
				return fmt.Errorf("%w: move constraints changed for range %d", ErrStaleRebalancePlan, action.RangeID)
			}
			if activeMigrationCount(fresh) >= uint64(policy.MaxConcurrentMigrationsCluster) {
				return fmt.Errorf("%w: migration capacity changed", ErrStaleRebalancePlan)
			}
		case RebalanceSplitRange:
			key, valid := medianSplitKey(metric)
			if !valid || !bytes.Equal(key, action.SplitKey) || !splitNodesHaveCapacity(fresh.Nodes, metric, policy) ||
				activeSplitCount(fresh) >= uint64(policy.MaxConcurrentSplitsCluster) || uint64(len(fresh.Ranges)) >= policy.MaxRanges {
				return fmt.Errorf("%w: split constraints changed for range %d", ErrStaleRebalancePlan, action.RangeID)
			}
		case RebalanceTransferLeader:
			target, targetOK := findNode(fresh.Nodes, action.TargetNodeID)
			replica, hosted := replicaOn(metric, action.TargetNodeID)
			if metric.Leader != action.SourceNodeID || !targetOK || !target.Healthy || !target.Available || !hosted ||
				!replica.Healthy || replica.Role != "VOTER" || replica.Lag != 0 || replica.LastApplied < metric.CommitIndex ||
				(policy.MaxLeaderCountPerNode != 0 && target.Leaders >= policy.MaxLeaderCountPerNode) {
				return fmt.Errorf("%w: leadership constraints changed for range %d", ErrStaleRebalancePlan, action.RangeID)
			}
		default:
			return fmt.Errorf("%w: unknown action type", ErrStaleRebalancePlan)
		}
	}
	return nil
}

func activeMigrationCount(snapshot RebalanceClusterSnapshot) uint64 {
	var count uint64
	for _, node := range snapshot.Nodes {
		count = saturatingAdd(count, node.MigrationsIn)
	}
	return count
}

func activeSplitCount(snapshot RebalanceClusterSnapshot) uint64 {
	var count uint64
	for _, metric := range snapshot.Ranges {
		if metric.Splitting {
			count++
		}
	}
	return count
}

func scoreNodes(nodes []RebalanceNodeMetric, policy RebalancePolicy) map[raft.NodeID]uint64 {
	result := make(map[raft.NodeID]uint64, len(nodes))
	var bytesTotal, writes, reads, leaders, backlog, replicas uint64
	for _, node := range nodes {
		bytesTotal = saturatingAdd(bytesTotal, node.LogicalBytes)
		writes = saturatingAdd(writes, node.WriteRate)
		reads = saturatingAdd(reads, node.ReadRate)
		leaders = saturatingAdd(leaders, node.Leaders)
		backlog = saturatingAdd(backlog, node.ApplyBacklog)
		replicas = saturatingAdd(replicas, node.HostedReplicas)
	}
	count := uint64(len(nodes))
	for _, node := range nodes {
		var score uint64
		score = saturatingAdd(score, weightedRatio(node.LogicalBytes, bytesTotal, count, policy.BytesWeight))
		score = saturatingAdd(score, weightedRatio(node.WriteRate, writes, count, policy.WriteWeight))
		score = saturatingAdd(score, weightedRatio(node.ReadRate, reads, count, policy.ReadWeight))
		score = saturatingAdd(score, weightedRatio(node.Leaders, leaders, count, policy.LeaderWeight))
		score = saturatingAdd(score, weightedRatio(node.ApplyBacklog, backlog, count, policy.BacklogWeight))
		score = saturatingAdd(score, weightedRatio(node.HostedReplicas, replicas, count, policy.ReplicaWeight))
		result[node.NodeID] = score
	}
	return result
}

func weightedRatio(value, total, count, weight uint64) uint64 {
	if value == 0 || total == 0 || weight == 0 {
		return 0
	}
	ratio := mulDivSaturating(value, saturatingMul(count, rateScale), total)
	return mulDivSaturating(ratio, weight, 1)
}

func saturatingMul(left, right uint64) uint64 {
	if left == 0 || right == 0 {
		return 0
	}
	if left > ^uint64(0)/right {
		return ^uint64(0)
	}
	return left * right
}

func rangeLoad(metric RebalanceRangeMetric, policy RebalancePolicy) uint64 {
	load := mulDivSaturating(metric.LogicalBytes, policy.RangeBytesWeight, 1)
	load = saturatingAdd(load, mulDivSaturating(metric.WriteRate, policy.RangeWriteWeight, 1))
	load = saturatingAdd(load, mulDivSaturating(metric.ReadRate, policy.RangeReadWeight, 1))
	return saturatingAdd(load, mulDivSaturating(metric.ApplyBacklog, policy.RangeBacklogWeight, 1))
}

func splitWarranted(metric RebalanceRangeMetric, policy RebalancePolicy, rateReady bool) bool {
	return metric.LogicalBytes >= policy.RangeSplitBytes || metric.LogicalBytes >= policy.MinRangeBytes && rateReady && (metric.ReadRate >= policy.RangeHotReadRate || metric.WriteRate >= policy.RangeHotWriteRate)
}

func splitReason(metric RebalanceRangeMetric, policy RebalancePolicy) RebalanceReason {
	if metric.LogicalBytes >= policy.RangeSplitBytes {
		return ReasonRangeTooLarge
	}
	return ReasonRangeTooHot
}

func medianSplitKey(metric RebalanceRangeMetric) ([]byte, bool) {
	keys := metric.UserKeys
	if len(keys) < 2 {
		return nil, false
	}
	unique := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(unique) == 0 || !bytes.Equal(unique[len(unique)-1], key) {
			unique = append(unique, key)
		}
	}
	if len(unique) < 2 {
		return nil, false
	}
	key := unique[len(unique)/2]
	if !metric.StartUnbounded && bytes.Compare(key, metric.StartKey) <= 0 || !metric.EndUnbounded && bytes.Compare(key, metric.EndKey) >= 0 {
		return nil, false
	}
	return append([]byte(nil), key...), true
}

func leaderCandidate(snapshot RebalanceClusterSnapshot, policy RebalancePolicy, scores map[raft.NodeID]uint64, beforePotential uint64, cooldowns RebalanceCooldowns) (RebalanceAction, bool) {
	nodes := append([]RebalanceNodeMetric(nil), snapshot.Nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Leaders != nodes[j].Leaders {
			return nodes[i].Leaders > nodes[j].Leaders
		}
		return nodes[i].NodeID < nodes[j].NodeID
	})
	if len(nodes) < 2 || nodes[0].Leaders <= nodes[len(nodes)-1].Leaders+1 {
		return RebalanceAction{}, false
	}
	source := nodes[0]
	for _, metric := range snapshot.Ranges {
		if metric.Leader != source.NodeID || metric.Splitting || metric.Migrating || !cooldownExpired(snapshot, cooldowns, metric.RangeID, source.NodeID) {
			continue
		}
		for _, replica := range metric.Replicas {
			if replica.NodeID == source.NodeID || !replica.Healthy || replica.Role != "VOTER" || replica.Lag != 0 || replica.LastApplied < metric.CommitIndex {
				continue
			}
			target, ok := findNode(snapshot.Nodes, replica.NodeID)
			if !ok || !target.Healthy || !target.Available || target.Leaders >= source.Leaders {
				continue
			}
			improvement := projectedPlacementImprovement(snapshot.Nodes, metric, source.NodeID, target.NodeID, policy, true, beforePotential)
			if improvement == 0 {
				continue
			}
			return RebalanceAction{Type: RebalanceTransferLeader, Reason: ReasonLeaderSkew, RangeID: metric.RangeID,
				RangeGeneration: metric.Generation, CatalogGeneration: snapshot.CatalogGeneration, SourceNodeID: source.NodeID,
				TargetNodeID: target.NodeID, SourceScore: scores[source.NodeID], TargetScore: scores[target.NodeID],
				EstimatedCost: 1, ExpectedImprovement: improvement,
				Constraints: []string{"existing healthy voter", "caught up through commit index", "range operation exclusion"}}, true
		}
	}
	return RebalanceAction{}, false
}

func moveCandidates(snapshot RebalanceClusterSnapshot, policy RebalancePolicy, scores map[raft.NodeID]uint64, beforePotential uint64, cooldowns RebalanceCooldowns) []rebalanceCandidate {
	nodes := append([]RebalanceNodeMetric(nil), snapshot.Nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		if scores[nodes[i].NodeID] != scores[nodes[j].NodeID] {
			return scores[nodes[i].NodeID] > scores[nodes[j].NodeID]
		}
		return nodes[i].NodeID < nodes[j].NodeID
	})
	if len(nodes) < 2 {
		return nil
	}
	if !moveOverloaded(nodes, policy, scores, cooldowns) {
		return nil
	}
	var result []rebalanceCandidate
	for _, source := range nodes {
		for _, metric := range snapshot.Ranges {
			if metric.Splitting || metric.Migrating || !metric.Active || moveRateWarmupRequired(metric, policy) || !cooldownExpired(snapshot, cooldowns, metric.RangeID, source.NodeID) {
				continue
			}
			replica, hosted := replicaOn(metric, source.NodeID)
			if !hosted || !replica.Healthy || replica.Role != "VOTER" {
				continue
			}
			for targetIndex := len(nodes) - 1; targetIndex >= 0; targetIndex-- {
				target := nodes[targetIndex]
				if !eligibleMoveTarget(metric, source, target, policy) {
					continue
				}
				improvement := projectedPlacementImprovement(snapshot.Nodes, metric, source.NodeID, target.NodeID, policy, false, beforePotential)
				result = append(result, rebalanceCandidate{order: 2, action: RebalanceAction{Type: RebalanceMoveReplica,
					Reason: ReasonNodeOverloaded, RangeID: metric.RangeID, RangeGeneration: metric.Generation,
					CatalogGeneration: snapshot.CatalogGeneration, SourceReplicaID: replica.ReplicaID, SourceNodeID: source.NodeID,
					TargetNodeID: target.NodeID, SourceScore: scores[source.NodeID], TargetScore: scores[target.NodeID],
					EstimatedCost: max(1, metric.PhysicalBytes), ExpectedImprovement: improvement,
					Constraints: []string{"no duplicate node placement", "healthy available target", "capacity and operation limits"}}})
			}
		}
	}
	return result
}

func moveOverloaded(nodes []RebalanceNodeMetric, policy RebalancePolicy, scores map[raft.NodeID]uint64, cooldowns RebalanceCooldowns) bool {
	if len(nodes) < 2 {
		return false
	}
	ordered := append([]RebalanceNodeMetric(nil), nodes...)
	sort.Slice(ordered, func(i, j int) bool {
		if scores[ordered[i].NodeID] != scores[ordered[j].NodeID] {
			return scores[ordered[i].NodeID] > scores[ordered[j].NodeID]
		}
		return ordered[i].NodeID < ordered[j].NodeID
	})
	weight := policy.BytesWeight + policy.WriteWeight + policy.ReadWeight + policy.LeaderWeight + policy.BacklogWeight + policy.ReplicaWeight
	thresholdPPM := policy.NodeImbalanceStartPPM
	if _, recovering := cooldowns.Nodes[ordered[0].NodeID]; recovering {
		thresholdPPM = policy.NodeImbalanceRecoveryPPM
	}
	threshold := mulDivSaturating(weight*rateScale, rateScale+thresholdPPM, rateScale)
	return scores[ordered[0].NodeID] > threshold
}

func moveRateWarmupRequired(metric RebalanceRangeMetric, policy RebalancePolicy) bool {
	return metric.Samples < policy.MinSamples && (policy.WriteWeight != 0 || policy.ReadWeight != 0)
}

func eligibleMoveTarget(metric RebalanceRangeMetric, source, target RebalanceNodeMetric, policy RebalancePolicy) bool {
	if source.NodeID == target.NodeID || !source.Healthy || !target.Healthy || !target.Available || target.Warming || target.MigrationsIn >= uint64(policy.MaxIncomingMigrationsPerNode) || source.MigrationsOut >= uint64(policy.MaxOutgoingMigrationsPerNode) {
		return false
	}
	if policy.MaxReplicaCountPerNode != 0 && target.HostedReplicas >= policy.MaxReplicaCountPerNode {
		return false
	}
	if policy.MaxLogicalBytesPerNode != 0 && target.LogicalBytes > policy.MaxLogicalBytesPerNode-maybeClamp(metric.LogicalBytes, policy.MaxLogicalBytesPerNode) {
		return false
	}
	if policy.MaxWriteRatePerNode != 0 && target.WriteRate > policy.MaxWriteRatePerNode-maybeClamp(metric.WriteRate, policy.MaxWriteRatePerNode) {
		return false
	}
	if policy.MaxLeaderCountPerNode != 0 && target.Leaders >= policy.MaxLeaderCountPerNode && metric.Leader == source.NodeID {
		return false
	}
	_, exists := replicaOn(metric, target.NodeID)
	return !exists
}

func maybeClamp(value, ceiling uint64) uint64 {
	if value > ceiling {
		return ceiling
	}
	return value
}

func replicaOn(metric RebalanceRangeMetric, nodeID raft.NodeID) (RebalanceReplicaMetric, bool) {
	for _, replica := range metric.Replicas {
		if replica.NodeID == nodeID {
			return replica, true
		}
	}
	return RebalanceReplicaMetric{}, false
}

func findNode(nodes []RebalanceNodeMetric, id raft.NodeID) (RebalanceNodeMetric, bool) {
	for _, node := range nodes {
		if node.NodeID == id {
			return node, true
		}
	}
	return RebalanceNodeMetric{}, false
}

func cooldownExpired(snapshot RebalanceClusterSnapshot, cooldowns RebalanceCooldowns, rangeID RangeID, nodeID raft.NodeID) bool {
	if until, exists := cooldowns.Ranges[rangeID]; exists && snapshot.SnapshotTime.Before(until) {
		return false
	}
	if until, exists := cooldowns.Nodes[nodeID]; exists && snapshot.SnapshotTime.Before(until) {
		return false
	}
	return true
}

func candidateLess(left, right rebalanceCandidate) bool {
	leftCost, rightCost := max(1, left.action.EstimatedCost), max(1, right.action.EstimatedCost)
	leftHigh, leftLow := bits.Mul64(left.action.ExpectedImprovement, rightCost)
	rightHigh, rightLow := bits.Mul64(right.action.ExpectedImprovement, leftCost)
	if leftHigh != rightHigh {
		return leftHigh > rightHigh
	}
	if leftLow != rightLow {
		return leftLow > rightLow
	}
	if left.order != right.order {
		return left.order < right.order
	}
	if left.action.RangeID != right.action.RangeID {
		return left.action.RangeID < right.action.RangeID
	}
	if left.action.SourceNodeID != right.action.SourceNodeID {
		return left.action.SourceNodeID < right.action.SourceNodeID
	}
	if left.action.TargetNodeID != right.action.TargetNodeID {
		return left.action.TargetNodeID < right.action.TargetNodeID
	}
	return bytes.Compare(left.action.SplitKey, right.action.SplitKey) < 0
}

func projectedPlacementImprovement(nodes []RebalanceNodeMetric, metric RebalanceRangeMetric, sourceID, targetID raft.NodeID, policy RebalancePolicy, leaderOnly bool, before uint64) uint64 {
	after := projectedLoadPotential(nodes, metric, sourceID, targetID, policy, leaderOnly)
	return subtractClamp(before, after)
}

type placementTotals struct {
	bytes, writes, reads, leaders, backlog, replicas uint64
}

// projectedLoadPotential evaluates a candidate without materializing a node
// slice or score map. Candidate scoring can invoke this thousands of times per
// controller cycle, so keeping it allocation-free matters at large range
// counts while leaving the integer scoring formula unchanged.
func projectedLoadPotential(nodes []RebalanceNodeMetric, metric RebalanceRangeMetric, sourceID, targetID raft.NodeID, policy RebalancePolicy, leaderOnly bool) uint64 {
	var totals placementTotals
	for _, original := range nodes {
		node := projectedNode(original, metric, sourceID, targetID, leaderOnly)
		totals.bytes = saturatingAdd(totals.bytes, node.LogicalBytes)
		totals.writes = saturatingAdd(totals.writes, node.WriteRate)
		totals.reads = saturatingAdd(totals.reads, node.ReadRate)
		totals.leaders = saturatingAdd(totals.leaders, node.Leaders)
		totals.backlog = saturatingAdd(totals.backlog, node.ApplyBacklog)
		totals.replicas = saturatingAdd(totals.replicas, node.HostedReplicas)
	}
	count := uint64(len(nodes))
	var scoreTotal uint64
	for _, original := range nodes {
		scoreTotal = saturatingAdd(scoreTotal, placementScore(projectedNode(original, metric, sourceID, targetID, leaderOnly), totals, count, policy))
	}
	if count == 0 {
		return 0
	}
	mean := scoreTotal / count
	var potential uint64
	for _, original := range nodes {
		score := placementScore(projectedNode(original, metric, sourceID, targetID, leaderOnly), totals, count, policy)
		difference := score - min(score, mean)
		if score < mean {
			difference = mean - score
		}
		potential = saturatingAdd(potential, saturatingMul(difference, difference))
	}
	return potential
}

func placementScore(node RebalanceNodeMetric, totals placementTotals, count uint64, policy RebalancePolicy) uint64 {
	var score uint64
	score = saturatingAdd(score, weightedRatio(node.LogicalBytes, totals.bytes, count, policy.BytesWeight))
	score = saturatingAdd(score, weightedRatio(node.WriteRate, totals.writes, count, policy.WriteWeight))
	score = saturatingAdd(score, weightedRatio(node.ReadRate, totals.reads, count, policy.ReadWeight))
	score = saturatingAdd(score, weightedRatio(node.Leaders, totals.leaders, count, policy.LeaderWeight))
	score = saturatingAdd(score, weightedRatio(node.ApplyBacklog, totals.backlog, count, policy.BacklogWeight))
	return saturatingAdd(score, weightedRatio(node.HostedReplicas, totals.replicas, count, policy.ReplicaWeight))
}

func projectedNode(node RebalanceNodeMetric, metric RebalanceRangeMetric, sourceID, targetID raft.NodeID, leaderOnly bool) RebalanceNodeMetric {
	switch node.NodeID {
	case sourceID:
		if leaderOnly {
			node.Leaders = subtractClamp(node.Leaders, 1)
			return node
		}
		node.HostedReplicas = subtractClamp(node.HostedReplicas, 1)
		node.LogicalBytes = subtractClamp(node.LogicalBytes, metric.LogicalBytes)
		node.ReadRate = subtractClamp(node.ReadRate, metric.ReadRate)
		node.WriteRate = subtractClamp(node.WriteRate, metric.WriteRate)
		node.RequestRate = subtractClamp(node.RequestRate, metric.RequestRate)
		node.ApplyBacklog = subtractClamp(node.ApplyBacklog, metric.ApplyBacklog)
		if metric.Leader == sourceID {
			node.Leaders = subtractClamp(node.Leaders, 1)
		}
	case targetID:
		if leaderOnly {
			node.Leaders = saturatingAdd(node.Leaders, 1)
			return node
		}
		node.HostedReplicas = saturatingAdd(node.HostedReplicas, 1)
		node.LogicalBytes = saturatingAdd(node.LogicalBytes, metric.LogicalBytes)
		node.ReadRate = saturatingAdd(node.ReadRate, metric.ReadRate)
		node.WriteRate = saturatingAdd(node.WriteRate, metric.WriteRate)
		node.RequestRate = saturatingAdd(node.RequestRate, metric.RequestRate)
		node.ApplyBacklog = saturatingAdd(node.ApplyBacklog, metric.ApplyBacklog)
		if metric.Leader == sourceID {
			node.Leaders = saturatingAdd(node.Leaders, 1)
		}
	}
	return node
}

func loadPotential(scores map[raft.NodeID]uint64) uint64 {
	if len(scores) == 0 {
		return 0
	}
	var total uint64
	for _, score := range scores {
		total = saturatingAdd(total, score)
	}
	mean := total / uint64(len(scores)) //nolint:gosec // non-empty map length is positive
	var potential uint64
	for _, score := range scores {
		difference := score - min(score, mean)
		if score < mean {
			difference = mean - score
		}
		potential = saturatingAdd(potential, saturatingMul(difference, difference))
	}
	return potential
}

func subtractClamp(value, amount uint64) uint64 {
	if amount > value {
		return 0
	}
	return value - amount
}
