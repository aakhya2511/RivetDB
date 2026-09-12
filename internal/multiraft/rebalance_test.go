//nolint:govet // tests intentionally keep operation errors scoped to their assertions
package multiraft

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
)

func TestRebalancePolicyValidation(t *testing.T) {
	policy := DefaultRebalancePolicy()
	if err := policy.Validate(); err != nil {
		t.Fatalf("default policy: %v", err)
	}
	policy.EWMAAlphaPPM = rateScale + 1
	if err := policy.Validate(); !errors.Is(err, ErrInvalidRebalancePolicy) {
		t.Fatalf("invalid alpha error = %v", err)
	}
}

func TestTelemetrySamplerEWMAAndReset(t *testing.T) {
	fake := clock.NewMock()
	sampler, err := NewTelemetrySampler(fake, 500_000)
	if err != nil {
		t.Fatal(err)
	}
	if read, _, _, samples, observeErr := sampler.Observe(7, TrafficCounters{Reads: 100}); observeErr != nil || read != 0 || samples != 0 {
		t.Fatalf("baseline = (%d,%d,%v)", read, samples, observeErr)
	}
	fake.Advance(time.Second)
	read, _, _, samples, err := sampler.Observe(7, TrafficCounters{Reads: 200})
	if err != nil || read != 100 || samples != 1 {
		t.Fatalf("first rate = (%d,%d,%v)", read, samples, err)
	}
	fake.Advance(time.Second)
	read, _, _, samples, err = sampler.Observe(7, TrafficCounters{Reads: 500})
	if err != nil || read != 200 || samples != 2 {
		t.Fatalf("EWMA = (%d,%d,%v)", read, samples, err)
	}
	fake.Advance(time.Second)
	read, _, _, samples, err = sampler.Observe(7, TrafficCounters{Reads: 1})
	if err != nil || read != 0 || samples != 0 {
		t.Fatalf("counter reset = (%d,%d,%v)", read, samples, err)
	}
	regressing := &manualRebalanceClock{now: clock.Epoch.Add(time.Hour)}
	regressionSampler, err := NewTelemetrySampler(regressing, 500_000)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, _, _ = regressionSampler.Observe(8, TrafficCounters{Reads: 1})
	regressing.now = clock.Epoch
	read, _, _, samples, err = regressionSampler.Observe(8, TrafficCounters{Reads: 2})
	if err != nil || read != 0 || samples != 0 {
		t.Fatalf("clock regression = (%d,%d,%v)", read, samples, err)
	}
}

func TestTelemetryRateOverflowSaturates(t *testing.T) {
	if rate := perSecond(^uint64(0), time.Nanosecond); rate != ^uint64(0) {
		t.Fatalf("overflow rate=%d", rate)
	}
	if got := mulDivSaturating(^uint64(0), rateScale, rateScale); got != ^uint64(0) {
		t.Fatalf("wide multiply/divide=%d", got)
	}
}

type manualRebalanceClock struct{ now time.Time }

func (c *manualRebalanceClock) Now() time.Time                     { return c.now }
func (c *manualRebalanceClock) Since(t time.Time) time.Duration    { return c.now.Sub(t) }
func (*manualRebalanceClock) NewTimer(time.Duration) clock.Timer   { panic("unused") }
func (*manualRebalanceClock) NewTicker(time.Duration) clock.Ticker { panic("unused") }
func (*manualRebalanceClock) After(time.Duration) <-chan time.Time { panic("unused") }
func (*manualRebalanceClock) Sleep(time.Duration)                  { panic("unused") }

func TestPlanRebalanceDeterministicUnderShuffledInput(t *testing.T) {
	policy := moveOnlyPolicy()
	snapshot := moveSnapshot(80, 20)
	first, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Nodes[0], snapshot.Nodes[1] = snapshot.Nodes[1], snapshot.Nodes[0]
	snapshot.Ranges[0].Replicas[0], snapshot.Ranges[0].Replicas[1] = snapshot.Ranges[0].Replicas[1], snapshot.Ranges[0].Replicas[0]
	second, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("plans differ:\n%+v\n%+v", first, second)
	}
	if len(first.Actions) != 1 || first.Actions[0].Type != RebalanceMoveReplica || first.Actions[0].SourceNodeID != 1 || first.Actions[0].TargetNodeID != 2 {
		t.Fatalf("unexpected plan: %+v", first)
	}
}

func TestPlanRebalanceHysteresisAndCooldown(t *testing.T) {
	policy := moveOnlyPolicy()
	balanced, err := PlanRebalance(moveSnapshot(51, 49), policy, RebalanceCooldowns{})
	if err != nil || len(balanced.Actions) != 0 {
		t.Fatalf("51/49 plan = %+v, %v", balanced, err)
	}
	hot := moveSnapshot(80, 20)
	plan, err := PlanRebalance(hot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 1 {
		t.Fatalf("80/20 plan = %+v, %v", plan, err)
	}
	cooldowns := RebalanceCooldowns{Ranges: map[RangeID]time.Time{10: hot.SnapshotTime.Add(time.Hour)}}
	cooled, err := PlanRebalance(hot, policy, cooldowns)
	if err != nil || len(cooled.Actions) != 0 {
		t.Fatalf("cooled plan = %+v, %v", cooled, err)
	}
	recovering := moveSnapshot(56, 44)
	recoveryCooldown := RebalanceCooldowns{Nodes: map[raft.NodeID]time.Time{1: recovering.SnapshotTime.Add(-time.Second)}}
	recoveryPlan, err := PlanRebalance(recovering, policy, recoveryCooldown)
	if err != nil || len(recoveryPlan.Actions) != 1 {
		t.Fatalf("recovery-threshold plan = %+v, %v", recoveryPlan, err)
	}
}

func TestPlanPrefersLeaderTransferForLeaderOnlySkew(t *testing.T) {
	policy := DefaultRebalancePolicy()
	policy.Moves, policy.Splits = false, false
	snapshot := moveSnapshot(10, 10)
	snapshot.Nodes = append(snapshot.Nodes, RebalanceNodeMetric{NodeID: 3, Healthy: true, Available: true})
	snapshot.Nodes[0].Leaders, snapshot.Nodes[1].Leaders = 3, 0
	snapshot.Ranges[0].Leader = 1
	snapshot.Ranges[0].Replicas[1].NodeID = 2
	snapshot.Ranges[0].Replicas[1].Role = "VOTER"
	snapshot.Ranges[0].Replicas[1].LastApplied = snapshot.Ranges[0].CommitIndex
	plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 1 || plan.Actions[0].Type != RebalanceTransferLeader || plan.Actions[0].TargetNodeID != 2 {
		t.Fatalf("leader plan = %+v, %v", plan, err)
	}
}

func TestPlanSplitUsesUserKeyMedianAndRejectsOneKey(t *testing.T) {
	policy := DefaultRebalancePolicy()
	policy.Moves, policy.Leaders = false, false
	policy.MinRangeBytes = 1
	policy.RangeSplitBytes = 10
	snapshot := moveSnapshot(10, 10)
	snapshot.Nodes = append(snapshot.Nodes, RebalanceNodeMetric{NodeID: 3, Healthy: true, Available: true})
	snapshot.Ranges[0].LogicalBytes = 100
	snapshot.Ranges[0].UserKeys = [][]byte{[]byte("z"), []byte("a"), []byte("m")}
	plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 1 || string(plan.Actions[0].SplitKey) != "m" {
		t.Fatalf("split plan = %+v, %v", plan, err)
	}
	snapshot.Ranges[0].UserKeys = [][]byte{[]byte("same"), []byte("same")}
	plan, err = PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 0 || plan.NoopReason != ReasonHotUnsplittableKeyspace {
		t.Fatalf("unsplittable plan = %+v, %v", plan, err)
	}
}

func TestHotTinyRangeDoesNotSplit(t *testing.T) {
	policy := DefaultRebalancePolicy()
	policy.Moves, policy.Leaders = false, false
	snapshot := moveSnapshot(10, 10)
	snapshot.Ranges[0].LogicalBytes = policy.MinRangeBytes - 1
	snapshot.Ranges[0].ReadRate = policy.RangeHotReadRate * 2
	snapshot.Ranges[0].UserKeys = [][]byte{[]byte("a"), []byte("z")}
	plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 0 {
		t.Fatalf("tiny hot plan=%+v err=%v", plan, err)
	}
}

func TestProjectedMoveConvergesWithoutReverseChurn(t *testing.T) {
	policy := moveOnlyPolicy()
	snapshot := moveSnapshot(80, 20)
	plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(plan.Actions) != 1 {
		t.Fatalf("initial plan=%+v err=%v", plan, err)
	}
	snapshot.Nodes[0].WriteRate, snapshot.Nodes[1].WriteRate = 50, 50
	snapshot.Ranges[0].Replicas[0].NodeID = 2
	converged, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil || len(converged.Actions) != 0 {
		t.Fatalf("converged plan=%+v err=%v", converged, err)
	}
}

func TestValidateRebalancePlanRejectsStaleCatalog(t *testing.T) {
	policy := moveOnlyPolicy()
	snapshot := moveSnapshot(80, 20)
	plan, err := PlanRebalance(snapshot, policy, RebalanceCooldowns{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot.CatalogGeneration++
	if err := ValidateRebalancePlan(plan, snapshot, policy); !errors.Is(err, ErrStaleRebalancePlan) {
		t.Fatalf("stale validation = %v", err)
	}
}

func TestRebalanceControlReplicatesAndCodecRoundTrips(t *testing.T) {
	catalog := mustCatalog(t, threeRangeBootstrap())
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: catalog.Snapshot().Nodes, Directory: t.TempDir(), Bootstrap: catalog})
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.SetRebalancePolicyVersion(t.Context(), 9); err != nil {
		t.Fatal(err)
	}
	epoch, err := meta.TakeoverRebalanceController(t.Context())
	if err != nil || epoch != 1 {
		t.Fatalf("epoch=%d err=%v", epoch, err)
	}
	at := clock.Epoch.Add(time.Hour)
	record, err := meta.BeginRebalanceAction(t.Context(), RebalanceAction{Type: RebalanceMoveReplica, Reason: ReasonNodeOverloaded,
		RangeID: 10, RangeGeneration: 1, CatalogGeneration: catalog.Generation(), SourceReplicaID: 101, SourceNodeID: 1, TargetNodeID: 4}, at)
	if err != nil {
		t.Fatal(err)
	}
	if epoch, err = meta.TakeoverRebalanceController(t.Context()); err != nil || epoch != 2 {
		t.Fatalf("takeover epoch=%d err=%v", epoch, err)
	}
	if _, err := meta.AdvanceRebalanceAction(t.Context(), record.Action.ActionID, RebalanceActionExecuting, 0, 0, "", at.Add(time.Second), time.Time{}, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	until := at.Add(time.Hour)
	if _, err := meta.AdvanceRebalanceAction(t.Context(), record.Action.ActionID, RebalanceActionSucceeded, 7, 0, "", at.Add(2*time.Second), until, until, until); err != nil {
		t.Fatal(err)
	}
	snapshot := meta.Snapshot().Rebalance
	if snapshot.PolicyVersion != 9 || snapshot.ControllerEpoch != 2 || snapshot.NextActionID != 2 || len(snapshot.History) != 1 || snapshot.History[0].State != RebalanceActionSucceeded || !snapshot.Cooldowns.Ranges[10].Equal(until) {
		t.Fatalf("control snapshot=%+v", snapshot)
	}
	state, err := newMetadataState(catalog)
	if err != nil {
		t.Fatal(err)
	}
	state.rebalance = snapshot
	encoded, err := encodeMetadata(state)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeMetadata(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !sameRebalanceControl(snapshot, decoded.rebalance) {
		t.Fatal("rebalance control codec changed state")
	}
	next, err := meta.BeginRebalanceActionWithLimit(t.Context(), RebalanceAction{Type: RebalanceTransferLeader, Reason: ReasonLeaderSkew,
		RangeID: 11, RangeGeneration: 2, CatalogGeneration: catalog.Generation(), SourceNodeID: 2, TargetNodeID: 3}, at.Add(3*time.Second), 1)
	if err != nil {
		t.Fatal(err)
	}
	trimmed := meta.Snapshot().Rebalance.History
	if next.Action.ActionID != 2 || len(trimmed) != 1 || trimmed[0].Action.ActionID != 2 {
		t.Fatalf("bounded history=%+v", trimmed)
	}
}

func TestMetadataDecoderAcceptsCertifiedPhase8Version(t *testing.T) {
	state, err := newMetadataState(mustCatalog(t, threeRangeBootstrap()))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeMetadata(state)
	if err != nil {
		t.Fatal(err)
	}
	controlBytes, err := appendRebalanceControl(nil, state.rebalance)
	if err != nil {
		t.Fatal(err)
	}
	phase8 := append([]byte(nil), encoded[:len(encoded)-len(controlBytes)]...)
	phase8[4], phase8[5] = 2, 0
	decoded, err := decodeMetadata(phase8)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.rebalance.NextActionID != 1 || decoded.rebalance.PolicyVersion != 0 || len(decoded.rebalance.History) != 0 {
		t.Fatalf("phase8 control defaults=%+v", decoded.rebalance)
	}
}

func TestAutomaticRebalanceUsesCertifiedMoveReplica(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 2)
	cluster.elect(12, 1)
	manager, meta := migrationManager(t, cluster)
	for _, account := range []struct{ key, value string }{{"a-bank", "500"}, {"g-bank", "500"}} {
		if _, err := cluster.router.PutMVCC(t.Context(), []byte(account.key), []byte(account.value)); err != nil {
			t.Fatal(err)
		}
	}
	historicalAt, err := cluster.router.PutMVCC(t.Context(), []byte("a-history"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.router.PutMVCC(t.Context(), []byte("a-history"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	gHistoricalAt, err := cluster.router.PutMVCC(t.Context(), []byte("g-history"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.router.PutMVCC(t.Context(), []byte("g-history"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	pHistoricalAt, err := cluster.router.PutMVCC(t.Context(), []byte("p-history"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.router.PutMVCC(t.Context(), []byte("p-history"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	transferCommitted := false
	manager.hook = func(stage MigrationHookStage, _ MigrationRecord) {
		if stage != LearnerAdded || transferCommitted {
			return
		}
		txn, beginErr := cluster.router.Begin(t.Context())
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		from, getErr := txn.Get(t.Context(), []byte("a-bank"))
		if getErr != nil {
			t.Fatal(getErr)
		}
		to, getErr := txn.Get(t.Context(), []byte("g-bank"))
		if getErr != nil {
			t.Fatal(getErr)
		}
		fromValue, _ := strconv.Atoi(string(from))
		toValue, _ := strconv.Atoi(string(to))
		if putErr := txn.Put([]byte("a-bank"), []byte(strconv.Itoa(fromValue-25))); putErr != nil {
			t.Fatal(putErr)
		}
		if putErr := txn.Put([]byte("g-bank"), []byte(strconv.Itoa(toValue+25))); putErr != nil {
			t.Fatal(putErr)
		}
		if commitErr := txn.Commit(t.Context()); commitErr != nil {
			t.Fatal(commitErr)
		}
		transferCommitted = true
	}
	fake := clock.NewMockAt(clock.Epoch)
	policy := moveOnlyPolicy()
	policy.WriteWeight, policy.ReplicaWeight = 0, 1
	policy.MinSamples = 2
	controller, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: fake, Policy: policy, Migration: manager})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Observe(t.Context()); err != nil {
		t.Fatal(err)
	}
	for sample := 0; sample < 2; sample++ {
		for write := 0; write < 8; write++ {
			key := []byte(fmt.Sprintf("a-%d-%d", sample, write))
			if _, err := cluster.router.PutMVCC(t.Context(), key, []byte("hot")); err != nil {
				t.Fatal(err)
			}
		}
		fake.Advance(time.Second)
		if _, err := controller.Observe(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := controller.RunCycle(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	control := meta.Snapshot().Rebalance
	if len(plan.Actions) != 1 || plan.Actions[0].Type != RebalanceMoveReplica || len(control.History) != 1 || control.History[0].State != RebalanceActionSucceeded || control.History[0].MigrationID == 0 {
		t.Fatalf("plan=%+v history=%+v", plan, control.History)
	}
	if !transferCommitted {
		t.Fatal("bank transfer was not exercised during automatic migration")
	}
	reader, err := cluster.router.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, key := range []string{"a-bank", "g-bank"} {
		value, getErr := reader.Get(t.Context(), []byte(key))
		if getErr != nil {
			t.Fatal(getErr)
		}
		balance, convertErr := strconv.Atoi(string(value))
		if convertErr != nil {
			t.Fatal(convertErr)
		}
		total += balance
	}
	if err := reader.Abort(t.Context()); err != nil {
		t.Fatal(err)
	}
	if total != 1000 {
		t.Fatalf("bank total=%d", total)
	}
	historyKey, historyTimestamp := []byte("a-history"), historicalAt
	switch plan.Actions[0].RangeID {
	case 11:
		historyKey, historyTimestamp = []byte("g-history"), gHistoricalAt
	case 12:
		historyKey, historyTimestamp = []byte("p-history"), pHistoricalAt
	}
	target, err := cluster.nodes[plan.Actions[0].TargetNodeID].Replica(plan.Actions[0].RangeID)
	if err != nil {
		t.Fatal(err)
	}
	old, err := target.GetAt(t.Context(), historyKey, historyTimestamp)
	if err != nil || string(old) != "v1" {
		t.Fatalf("historical value=%q err=%v", old, err)
	}
}

func TestAutomaticRebalanceUsesCertifiedSplitRange(t *testing.T) {
	cluster, meta, split := newSplitHarness(t)
	cluster.elect(10, 1)
	for _, key := range []string{"a", "c", "e"} {
		if _, err := cluster.router.PutMVCC(t.Context(), []byte(key), []byte("large")); err != nil {
			t.Fatal(err)
		}
	}
	policy := DefaultRebalancePolicy()
	policy.Moves, policy.Leaders = false, false
	policy.MinRangeBytes, policy.RangeSplitBytes = 1, 1
	controller, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: clock.NewMock(), Policy: policy, Split: split})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	plan, err := controller.RunCycle(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	control := meta.Snapshot().Rebalance
	if len(plan.Actions) != 1 || plan.Actions[0].Type != RebalanceSplitRange || len(control.History) != 1 || control.History[0].State != RebalanceActionSucceeded || control.History[0].SplitID == 0 {
		t.Fatalf("plan=%+v history=%+v", plan, control.History)
	}
}

func TestAutomaticRebalanceUsesCertifiedLeadershipTransfer(t *testing.T) {
	cluster := newMigrationCluster(t)
	for _, rangeID := range []RangeID{10, 11, 12} {
		cluster.elect(rangeID, 3)
	}
	_, meta := migrationManager(t, cluster)
	policy := DefaultRebalancePolicy()
	policy.Moves, policy.Splits = false, false
	controller, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: clock.NewMock(), Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	plan, err := controller.RunCycle(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	control := meta.Snapshot().Rebalance
	if len(plan.Actions) != 1 || plan.Actions[0].Type != RebalanceTransferLeader || len(control.History) != 1 || control.History[0].State != RebalanceActionSucceeded {
		t.Fatalf("plan=%+v history=%+v", plan, control.History)
	}
	target, err := cluster.nodes[plan.Actions[0].TargetNodeID].Replica(plan.Actions[0].RangeID)
	if err != nil || target.Status().Raft.Role != raft.Leader {
		t.Fatalf("target role err=%v", err)
	}
}

func TestRebalanceControllerRestartReconcilesCompletedSplit(t *testing.T) {
	cluster, meta, split := newSplitHarness(t)
	cluster.elect(10, 1)
	for _, key := range []string{"a", "c", "e"} {
		if _, err := cluster.router.PutMVCC(t.Context(), []byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	policy := DefaultRebalancePolicy()
	policy.Moves, policy.Leaders = false, false
	policy.MinRangeBytes, policy.RangeSplitBytes = 1, 1
	fake := clock.NewMock()
	crashing, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: fake, Policy: policy, Split: split,
		Hook: func(stage RebalanceHookStage, _ RebalanceActionRecord) {
			if stage == RebalanceCertifiedOperationReturned {
				panic("controller crash")
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crashing.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("controller did not crash at injected stage")
			}
		}()
		_, _ = crashing.RunCycle(t.Context())
	}()
	before := meta.Snapshot().Rebalance
	if len(before.History) != 1 || before.History[0].State != RebalanceActionExecuting {
		t.Fatalf("pre-restart history=%+v", before.History)
	}
	restarted, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: fake, Policy: policy, Split: split})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	after := meta.Snapshot().Rebalance
	if after.ControllerEpoch != before.ControllerEpoch+1 || after.History[0].State != RebalanceActionSucceeded || after.History[0].SplitID == 0 || after.Cooldowns.Ranges[10].IsZero() {
		t.Fatalf("post-restart control=%+v", after)
	}
}

func TestRebalanceControllerStopPreventsNewPlanning(t *testing.T) {
	cluster := newMigrationCluster(t)
	_, meta := migrationManager(t, cluster)
	controller, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: clock.NewMock(), Policy: DefaultRebalancePolicy()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	controller.Stop()
	if _, err := controller.RunCycle(t.Context()); !errors.Is(err, ErrRebalanceDisabled) {
		t.Fatalf("stopped run=%v", err)
	}
}

func TestMetaRangeUnavailabilityStopsControllerNotDataPlane(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	_, meta := migrationManager(t, cluster)
	controller, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: clock.NewMock(), Policy: DefaultRebalancePolicy()})
	if err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range cluster.bootstrap.Nodes {
		if err := meta.StopNode(nodeID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := controller.Activate(t.Context()); err == nil {
		t.Fatal("controller activated without MetaRange quorum")
	}
	if _, err := cluster.router.PutMVCC(t.Context(), []byte("a-data-plane"), []byte("available")); err != nil {
		t.Fatalf("user range stopped with MetaRange: %v", err)
	}
}

func moveOnlyPolicy() RebalancePolicy {
	policy := DefaultRebalancePolicy()
	policy.Splits, policy.Leaders = false, false
	policy.BytesWeight, policy.ReadWeight, policy.LeaderWeight, policy.BacklogWeight, policy.ReplicaWeight = 0, 0, 0, 0, 0
	policy.WriteWeight = 1
	return policy
}

func moveSnapshot(leftWrite, rightWrite uint64) RebalanceClusterSnapshot {
	rangeWrite := uint64(0)
	if leftWrite > rightWrite {
		rangeWrite = (leftWrite - rightWrite) / 2
	}
	return RebalanceClusterSnapshot{SnapshotTime: clock.Epoch.Add(time.Hour), CatalogGeneration: 4, PolicyVersion: 1, ControllerEpoch: 2,
		Nodes: []RebalanceNodeMetric{{NodeID: 1, HostedReplicas: 1, Leaders: 1, WriteRate: leftWrite, Healthy: true, Available: true},
			{NodeID: 2, HostedReplicas: 1, Leaders: 1, WriteRate: rightWrite, Healthy: true, Available: true}},
		Ranges: []RebalanceRangeMetric{{RangeID: 10, Generation: 3, LogicalBytes: 100, PhysicalBytes: 200, WriteRate: rangeWrite,
			Samples: 3, Leader: 1, Active: true, StartUnbounded: true, EndUnbounded: true,
			Replicas: []RebalanceReplicaMetric{{RangeID: 10, ReplicaID: 100, NodeID: raft.NodeID(1), Role: "VOTER", Healthy: true},
				{RangeID: 10, ReplicaID: 101, NodeID: raft.NodeID(3), Role: "VOTER", Healthy: true}}}}}
}
