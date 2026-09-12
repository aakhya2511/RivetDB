package multiraft

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
)

func TestRebalanceExactAntiOscillationSequence(t *testing.T) {
	policy := moveOnlyPolicy()
	sequence := [][2]uint64{{51, 49}, {49, 51}, {52, 48}, {48, 52}}
	actions := 0
	for _, load := range sequence {
		plan, err := PlanRebalance(moveSnapshot(load[0], load[1]), policy, RebalanceCooldowns{})
		if err != nil {
			t.Fatalf("load=%v: %v", load, err)
		}
		actions += len(plan.Actions)
		t.Logf("load=%d/%d actions=%d reason=%s", load[0], load[1], len(plan.Actions), plan.NoopReason)
	}
	if actions != 0 {
		t.Fatalf("minor alternating actions=%d", actions)
	}

	sustained := moveSnapshot(80, 20)
	plan, err := PlanRebalance(sustained, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 1 {
		t.Fatalf("sustained plan=%+v err=%v", plan, err)
	}
	action := plan.Actions[0]
	applyModeledMove(&sustained, action)
	protectedUntil := sustained.SnapshotTime.Add(policy.Cooldown)
	cooldowns := RebalanceCooldowns{Ranges: map[RangeID]time.Time{action.RangeID: protectedUntil}}
	sustained.Nodes[0].WriteRate, sustained.Nodes[1].WriteRate = 20, 80
	sustained.Ranges[0].WriteRate = 30
	reverse, err := PlanRebalance(sustained, policy, cooldowns)
	if err != nil || len(reverse.Actions) != 0 {
		t.Fatalf("protected reverse=%+v err=%v", reverse, err)
	}
	t.Logf("load=80/20 actions=1 range=%d source=%d target=%d reverseMovesInsideProtectedWindow=0", action.RangeID, action.SourceNodeID, action.TargetNodeID)
}

func TestRebalanceTransientSpikeVersusSustainedLoad(t *testing.T) {
	policy := moveOnlyPolicy()
	policy.EWMAAlphaPPM = 100_000
	policy.NodeImbalanceStartPPM = 400_000
	policy.NodeImbalanceRecoveryPPM = 200_000
	policy.MinSamples = 3
	fake := clock.NewMock()
	left, err := NewTelemetrySampler(fake, policy.EWMAAlphaPPM)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewTelemetrySampler(fake, policy.EWMAAlphaPPM)
	if err != nil {
		t.Fatal(err)
	}
	leftCount, rightCount := uint64(100), uint64(100)
	_, _, _, _, _ = left.Observe(1, TrafficCounters{Writes: leftCount})
	_, _, _, _, _ = right.Observe(2, TrafficCounters{Writes: rightCount})
	observe := func(leftDelta, rightDelta uint64) (uint64, uint64, uint32) {
		fake.Advance(time.Second)
		leftCount, rightCount = leftCount+leftDelta, rightCount+rightDelta
		_, leftRate, _, samples, observeErr := left.Observe(1, TrafficCounters{Writes: leftCount})
		if observeErr != nil {
			t.Fatal(observeErr)
		}
		_, rightRate, _, _, observeErr := right.Observe(2, TrafficCounters{Writes: rightCount})
		if observeErr != nil {
			t.Fatal(observeErr)
		}
		return leftRate, rightRate, samples
	}
	for _, delta := range [][2]uint64{{100, 100}, {400, 100}, {100, 100}, {100, 100}} {
		leftRate, rightRate, samples := observe(delta[0], delta[1])
		snapshot := moveSnapshot(leftRate, rightRate)
		snapshot.Ranges[0].Samples = samples
		plan, planErr := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
		if planErr != nil || len(plan.Actions) != 0 {
			t.Fatalf("transient delta=%v rates=%d/%d samples=%d plan=%+v err=%v", delta, leftRate, rightRate, samples, plan, planErr)
		}
	}
	triggered := false
	for sample := 0; sample < 20; sample++ {
		leftRate, rightRate, samples := observe(1000, 100)
		snapshot := moveSnapshot(leftRate, rightRate)
		snapshot.Ranges[0].Samples = samples
		plan, planErr := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
		if planErr != nil {
			t.Fatal(planErr)
		}
		if len(plan.Actions) == 1 {
			triggered = true
			t.Logf("sustained sample=%d rates=%d/%d action=%+v", sample+1, leftRate, rightRate, plan.Actions[0])
			break
		}
	}
	if !triggered {
		t.Fatal("sustained imbalance did not trigger")
	}
}

func TestRebalanceRepeatedConvergenceAndLongNoop(t *testing.T) {
	policy := moveOnlyPolicy()
	snapshot := modeledConvergenceSnapshot()
	initial := loadPotential(scoreNodes(snapshot.Nodes, policy))
	previous := initial
	actions := 0
	for cycle := 1; cycle <= 20; cycle++ {
		plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Actions) == 0 {
			t.Logf("cycle=%d score=%d action=NOOP reason=%s", cycle, previous, plan.NoopReason)
			break
		}
		if err := ValidateRebalancePlan(plan, snapshot, policy); err != nil {
			t.Fatal(err)
		}
		action := plan.Actions[0]
		applyModeledMove(&snapshot, action)
		after := loadPotential(scoreNodes(snapshot.Nodes, policy))
		if after >= previous {
			t.Fatalf("cycle=%d score did not improve: %d -> %d", cycle, previous, after)
		}
		t.Logf("cycle=%d score=%d action=MOVE range=%d source=%d target=%d reason=%s expected=%d actual=%d postScore=%d",
			cycle, previous, action.RangeID, action.SourceNodeID, action.TargetNodeID, action.Reason, action.ExpectedImprovement, previous-after, after)
		previous, actions = after, actions+1
	}
	if actions < 2 || previous >= initial {
		t.Fatalf("actions=%d initial=%d final=%d", actions, initial, previous)
	}
	for cycle := 0; cycle < 100; cycle++ {
		plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
		if err != nil || len(plan.Actions) != 0 || plan.NoopReason != ReasonBalanced {
			t.Fatalf("stable cycle=%d plan=%+v err=%v", cycle, plan, err)
		}
	}
	t.Logf("initialScore=%d finalScore=%d actions=%d unchangedNOOPCycles=100", initial, previous, actions)
}

func TestRebalanceBalancedClusterThousandNoopCycles(t *testing.T) {
	policy := moveOnlyPolicy()
	for cycle := 0; cycle < 1000; cycle++ {
		plan, err := PlanRebalance(moveSnapshot(50, 50), policy, RebalanceCooldowns{})
		if err != nil || len(plan.Actions) != 0 || plan.NoopReason != ReasonBalanced {
			t.Fatalf("cycle=%d plan=%+v err=%v", cycle, plan, err)
		}
	}
	t.Log("cycles=1000 moves=0 splits=0 leaderTransfers=0")
}

func TestRebalanceCounterResetAndClockRegressionSuppressActions(t *testing.T) {
	manual := &manualRebalanceClock{now: clock.Epoch.Add(100 * time.Second)}
	sampler, err := NewTelemetrySampler(manual, 500_000)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, _, _ = sampler.Observe(1, TrafficCounters{Writes: 100})
	manual.now = clock.Epoch.Add(101 * time.Second)
	_, rate, _, samples, err := sampler.Observe(1, TrafficCounters{Writes: 200})
	if err != nil || rate != 100 || samples != 1 {
		t.Fatalf("rising rate=%d samples=%d err=%v", rate, samples, err)
	}
	manual.now = clock.Epoch.Add(99 * time.Second)
	_, rate, _, samples, err = sampler.Observe(1, TrafficCounters{Writes: 201})
	if err != nil || rate != 0 || samples != 0 {
		t.Fatalf("regression rate=%d samples=%d err=%v", rate, samples, err)
	}
	manual.now = clock.Epoch.Add(100 * time.Second)
	_, rate, _, samples, err = sampler.Observe(1, TrafficCounters{Writes: 1})
	if err != nil || rate != 0 || samples != 0 {
		t.Fatalf("reset rate=%d samples=%d err=%v", rate, samples, err)
	}
	policy := moveOnlyPolicy()
	snapshot := moveSnapshot(rate, 0)
	snapshot.SnapshotTime = manual.now
	snapshot.Ranges[0].Samples = samples
	plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{Ranges: map[RangeID]time.Time{10: clock.Epoch.Add(102 * time.Second)}})
	if err != nil || len(plan.Actions) != 0 {
		t.Fatalf("phantom plan=%+v err=%v", plan, err)
	}
}

func TestRebalanceUnsatisfiableTopologyRemainsNoop(t *testing.T) {
	policy := moveOnlyPolicy()
	snapshot := moveSnapshot(90, 10)
	snapshot.Nodes = append(snapshot.Nodes, RebalanceNodeMetric{NodeID: 3, WriteRate: 10, Healthy: true, Available: true})
	snapshot.Ranges[0].Replicas = []RebalanceReplicaMetric{
		{RangeID: 10, ReplicaID: 101, NodeID: 1, Role: "VOTER", Healthy: true},
		{RangeID: 10, ReplicaID: 102, NodeID: 2, Role: "VOTER", Healthy: true},
		{RangeID: 10, ReplicaID: 103, NodeID: 3, Role: "VOTER", Healthy: true},
	}
	for cycle := 0; cycle < 100; cycle++ {
		plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
		if err != nil || len(plan.Actions) != 0 || plan.NoopReason != ReasonNoEligibleTarget {
			t.Fatalf("cycle=%d plan=%+v err=%v", cycle, plan, err)
		}
	}
}

func TestAutomaticMigrationNodeFailureReconcilesOneAuthoritativeOperation(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	_, meta := migrationManager(t, cluster)
	failedNode := raft.NodeID(0)
	manager, err := NewMigrationManager(MigrationManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage MigrationHookStage, record MigrationRecord) {
		if stage == LearnerAdded && failedNode == 0 {
			failedNode = record.TargetNodeID
			cluster.scheduler.RemoveNode(failedNode)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	policy := moveOnlyPolicy()
	policy.WriteWeight, policy.ReplicaWeight = 0, 1
	controller, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: clock.NewMock(), Policy: policy, Migration: manager})
	if err != nil {
		t.Fatal(err)
	}
	if _, activateErr := controller.Activate(t.Context()); activateErr != nil {
		t.Fatal(activateErr)
	}
	_, runErr := controller.RunCycle(t.Context())
	if runErr == nil || failedNode == 0 {
		t.Fatalf("failure was not injected: node=%d err=%v", failedNode, runErr)
	}
	before := meta.Snapshot()
	t.Logf("runErr=%v migrations=%+v", runErr, before.Migrations)
	if len(before.Migrations) != 1 || len(before.Rebalance.History) != 1 || before.Rebalance.History[0].State != RebalanceActionExecuting || before.Rebalance.History[0].MigrationID != before.Migrations[0].MigrationID {
		t.Fatalf("pre-recovery metadata=%+v", before.Rebalance.History)
	}
	if err := cluster.scheduler.AddNode(cluster.nodes[failedNode]); err != nil {
		t.Fatal(err)
	}
	if err := controller.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	after := meta.Snapshot()
	if len(after.Migrations) != 1 || len(after.Rebalance.History) != 1 || after.Rebalance.History[0].State != RebalanceActionSucceeded || after.Rebalance.History[0].MigrationID != before.Migrations[0].MigrationID {
		t.Fatalf("post-recovery metadata=%+v migrations=%+v", after.Rebalance.History, after.Migrations)
	}
	t.Logf("ActionID=%d MigrationID=%d crashStage=LEARNER_ADDED recovery=SUCCEEDED duplicateOperations=0",
		after.Rebalance.History[0].Action.ActionID, after.Migrations[0].MigrationID)
}

func TestAutomaticSplitNodeFailureReconcilesOneAuthoritativeOperation(t *testing.T) {
	cluster, meta, _ := newSplitHarness(t)
	cluster.elect(10, 1)
	for _, key := range []string{"a", "c", "e"} {
		if _, err := cluster.router.PutMVCC(t.Context(), []byte(key), []byte("split")); err != nil {
			t.Fatal(err)
		}
	}
	failed := false
	manager, err := NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage SplitHookStage, _ SplitRecord) {
		if stage == ParentSplitFenceActive && !failed {
			failed = true
			cluster.scheduler.RemoveNode(1)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	policy := DefaultRebalancePolicy()
	policy.Moves, policy.Leaders = false, false
	policy.MinRangeBytes, policy.RangeSplitBytes = 1, 1
	controller, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: clock.NewMock(), Policy: policy, Split: manager})
	if err != nil {
		t.Fatal(err)
	}
	if _, activateErr := controller.Activate(t.Context()); activateErr != nil {
		t.Fatal(activateErr)
	}
	_, runErr := controller.RunCycle(t.Context())
	if runErr == nil || !failed {
		t.Fatalf("failure was not injected: failed=%v err=%v", failed, runErr)
	}
	before := meta.Snapshot()
	t.Logf("runErr=%v splits=%+v", runErr, before.Splits)
	if len(before.Splits) != 1 || len(before.Rebalance.History) != 1 || before.Rebalance.History[0].State != RebalanceActionExecuting || before.Rebalance.History[0].SplitID != before.Splits[0].SplitID {
		t.Fatalf("pre-recovery history=%+v splits=%+v", before.Rebalance.History, before.Splits)
	}
	if err := cluster.scheduler.AddNode(cluster.nodes[1]); err != nil {
		t.Fatal(err)
	}
	if err := controller.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	after := meta.Snapshot()
	if len(after.Splits) != 1 || len(after.Rebalance.History) != 1 || after.Rebalance.History[0].State != RebalanceActionSucceeded || after.Rebalance.History[0].SplitID != before.Splits[0].SplitID {
		t.Fatalf("post-recovery history=%+v splits=%+v", after.Rebalance.History, after.Splits)
	}
	t.Logf("ActionID=%d SplitID=%d crashStage=PARENT_FENCED recovery=SUCCEEDED duplicateOperations=0",
		after.Rebalance.History[0].Action.ActionID, after.Splits[0].SplitID)
}

func TestRecoveredNodeRequiresStableWarmupBeforePlacement(t *testing.T) {
	cluster := newMigrationCluster(t)
	_, meta := migrationManager(t, cluster)
	policy := moveOnlyPolicy()
	policy.MinSamples = 3
	controller, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: clock.NewMock(), Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if _, activateErr := controller.Activate(t.Context()); activateErr != nil {
		t.Fatal(activateErr)
	}
	cluster.scheduler.RemoveNode(4)
	failed, err := controller.Observe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if node := findRebalanceNode(t, failed, 4); node.Available || node.Warming {
		t.Fatalf("failed node=%+v", node)
	}
	if addErr := cluster.scheduler.AddNode(cluster.nodes[4]); addErr != nil {
		t.Fatal(addErr)
	}
	for sample := uint32(1); sample < policy.MinSamples; sample++ {
		snapshot, observeErr := controller.Observe(t.Context())
		if observeErr != nil {
			t.Fatal(observeErr)
		}
		if node := findRebalanceNode(t, snapshot, 4); !node.Warming {
			t.Fatalf("sample=%d recovered node prematurely eligible: %+v", sample, node)
		}
	}
	ready, err := controller.Observe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if node := findRebalanceNode(t, ready, 4); node.Warming || !node.Healthy || !node.Available {
		t.Fatalf("warmed node=%+v", node)
	}
	plannerSnapshot := moveSnapshot(80, 20)
	plannerSnapshot.Nodes[1].Warming = true
	plan, err := PlanRebalance(plannerSnapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 0 {
		t.Fatalf("warming target plan=%+v err=%v", plan, err)
	}
	plannerSnapshot.Nodes[1].Warming = false
	plan, err = PlanRebalance(plannerSnapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 1 {
		t.Fatalf("warmed target plan=%+v err=%v", plan, err)
	}
}

func TestLeaderSkewLongRunUsesTransfersOnly(t *testing.T) {
	policy := DefaultRebalancePolicy()
	policy.Moves, policy.Splits = false, false
	snapshot := RebalanceClusterSnapshot{SnapshotTime: clock.Epoch, CatalogGeneration: 1, PolicyVersion: 1, ControllerEpoch: 1,
		Nodes: []RebalanceNodeMetric{{NodeID: 1, Leaders: 10, Healthy: true, Available: true}, {NodeID: 2, Healthy: true, Available: true}, {NodeID: 3, Healthy: true, Available: true}}}
	for index := 0; index < 10; index++ {
		id := RangeID(index + 1)
		snapshot.Ranges = append(snapshot.Ranges, RebalanceRangeMetric{RangeID: id, Generation: 1, Active: true, Leader: 1, CommitIndex: 10,
			Replicas: []RebalanceReplicaMetric{{RangeID: id, ReplicaID: ReplicaID(index*10 + 1), NodeID: 1, Role: "VOTER", Healthy: true, LastApplied: 10},
				{RangeID: id, ReplicaID: ReplicaID(index*10 + 2), NodeID: 2, Role: "VOTER", Healthy: true, LastApplied: 10},
				{RangeID: id, ReplicaID: ReplicaID(index*10 + 3), NodeID: 3, Role: "VOTER", Healthy: true, LastApplied: 10}}})
	}
	transfers := 0
	for cycle := 0; cycle < 20; cycle++ {
		plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Actions) == 0 {
			break
		}
		action := plan.Actions[0]
		if action.Type != RebalanceTransferLeader {
			t.Fatalf("physical action=%+v", action)
		}
		for index := range snapshot.Nodes {
			if snapshot.Nodes[index].NodeID == action.SourceNodeID {
				snapshot.Nodes[index].Leaders--
			}
			if snapshot.Nodes[index].NodeID == action.TargetNodeID {
				snapshot.Nodes[index].Leaders++
			}
		}
		for index := range snapshot.Ranges {
			if snapshot.Ranges[index].RangeID == action.RangeID {
				snapshot.Ranges[index].Leader = action.TargetNodeID
			}
		}
		transfers++
	}
	for cycle := 0; cycle < 100; cycle++ {
		plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
		if err != nil || len(plan.Actions) != 0 {
			t.Fatalf("stable cycle=%d plan=%+v err=%v", cycle, plan, err)
		}
	}
	t.Logf("initialLeaders=10/0/0 finalLeaders=%d/%d/%d leaderTransfers=%d physicalMigrations=0 stableNOOPCycles=100",
		snapshot.Nodes[0].Leaders, snapshot.Nodes[1].Leaders, snapshot.Nodes[2].Leaders, transfers)
}

func TestManualOperationsSuppressConflictingAutomaticActions(t *testing.T) {
	policy := DefaultRebalancePolicy()
	snapshot := campaignSplitSnapshot()
	snapshot.Ranges[0].Migrating = true
	plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 0 || plan.NoopReason != ReasonOperationInProgress {
		t.Fatalf("manual migration plan=%+v err=%v", plan, err)
	}
	snapshot.Ranges[0].Migrating, snapshot.Ranges[0].Splitting = false, true
	plan, err = PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 0 || plan.NoopReason != ReasonOperationInProgress {
		t.Fatalf("manual split plan=%+v err=%v", plan, err)
	}
}

func TestControllerRejectsFailedTargetBeforeExecution(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	_, meta := migrationManager(t, cluster)
	policy := moveOnlyPolicy()
	policy.WriteWeight, policy.ReplicaWeight = 0, 1
	var target raft.NodeID
	controller, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: clock.NewMock(), Policy: policy,
		Hook: func(stage RebalanceHookStage, _ RebalanceActionRecord) {
			if stage == RebalancePlanBuilt && target != 0 {
				cluster.scheduler.RemoveNode(target)
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	if _, activateErr := controller.Activate(t.Context()); activateErr != nil {
		t.Fatal(activateErr)
	}
	dryRun, err := controller.Plan(t.Context())
	if err != nil || len(dryRun.Actions) != 1 {
		t.Fatalf("dry plan=%+v err=%v", dryRun, err)
	}
	target = dryRun.Actions[0].TargetNodeID
	_, runErr := controller.RunCycle(t.Context())
	if !errors.Is(runErr, ErrStaleRebalancePlan) {
		t.Fatalf("run error=%v", runErr)
	}
	metadata := meta.Snapshot()
	if len(metadata.Migrations) != 0 || len(metadata.Rebalance.History) != 0 {
		t.Fatalf("stale target submitted work: migrations=%d history=%d", len(metadata.Migrations), len(metadata.Rebalance.History))
	}
	if err := cluster.scheduler.AddNode(cluster.nodes[target]); err != nil {
		t.Fatal(err)
	}
	t.Logf("plannedTarget=%d failedBeforeValidate=true rejected=%v MoveReplicaSubmitted=0", target, runErr)
}

func findRebalanceNode(t testing.TB, snapshot RebalanceClusterSnapshot, nodeID raft.NodeID) RebalanceNodeMetric {
	t.Helper()
	for _, node := range snapshot.Nodes {
		if node.NodeID == nodeID {
			return node
		}
	}
	t.Fatalf("missing node %d", nodeID)
	return RebalanceNodeMetric{}
}

func modeledConvergenceSnapshot() RebalanceClusterSnapshot {
	snapshot := RebalanceClusterSnapshot{SnapshotTime: clock.Epoch.Add(time.Hour), CatalogGeneration: 1, PolicyVersion: 1, ControllerEpoch: 1,
		Nodes: []RebalanceNodeMetric{{NodeID: 1, WriteRate: 100, Healthy: true, Available: true}, {NodeID: 2, Healthy: true, Available: true}}}
	for index := 0; index < 5; index++ {
		rangeID := RangeID(index + 1)
		snapshot.Ranges = append(snapshot.Ranges, RebalanceRangeMetric{RangeID: rangeID, Generation: 1, Active: true, Samples: 3,
			LogicalBytes: 10, PhysicalBytes: 10, WriteRate: 20,
			Replicas: []RebalanceReplicaMetric{{RangeID: rangeID, ReplicaID: ReplicaID(index + 1), NodeID: 1, Role: "VOTER", Healthy: true},
				{RangeID: rangeID, ReplicaID: ReplicaID(index + 101), NodeID: 3, Role: "VOTER", Healthy: true}}})
		snapshot.Nodes[0].HostedReplicas++
	}
	return snapshot
}

func applyModeledMove(snapshot *RebalanceClusterSnapshot, action RebalanceAction) {
	var metric *RebalanceRangeMetric
	for index := range snapshot.Ranges {
		if snapshot.Ranges[index].RangeID == action.RangeID {
			metric = &snapshot.Ranges[index]
			break
		}
	}
	if metric == nil {
		panic(fmt.Sprintf("missing modeled range %d", action.RangeID))
	}
	for index := range metric.Replicas {
		if metric.Replicas[index].ReplicaID == action.SourceReplicaID {
			metric.Replicas[index].NodeID = action.TargetNodeID
		}
	}
	for index := range snapshot.Nodes {
		node := &snapshot.Nodes[index]
		switch node.NodeID {
		case action.SourceNodeID:
			node.HostedReplicas = subtractClamp(node.HostedReplicas, 1)
			node.LogicalBytes = subtractClamp(node.LogicalBytes, metric.LogicalBytes)
			node.WriteRate = subtractClamp(node.WriteRate, metric.WriteRate)
		case action.TargetNodeID:
			node.HostedReplicas++
			node.LogicalBytes = saturatingAdd(node.LogicalBytes, metric.LogicalBytes)
			node.WriteRate = saturatingAdd(node.WriteRate, metric.WriteRate)
		}
	}
}
