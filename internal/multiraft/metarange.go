package multiraft

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/rivetdb/rivetdb/internal/raft"
)

type SplitHookStage uint8

const (
	MetaSplitRecordDurable SplitHookStage = iota
	ParentSplitFenceActive
	TxnDrainComplete
	BootstrapBarrierDurable
	LeftImageDurable
	RightImageDurable
	ChildQuorumReady
	DeltaReplayProgress
	FinalFenceDurable
	ChildrenReplayedThroughFence
	MetaCutoverDurable
	ChildActivated
	ParentRetired
)

func (s SplitHookStage) String() string {
	names := [...]string{"META_SPLIT_RECORD_DURABLE", "PARENT_SPLIT_FENCE_ACTIVE", "TXN_DRAIN_COMPLETE", "BOOTSTRAP_BARRIER_DURABLE", "LEFT_IMAGE_DURABLE", "RIGHT_IMAGE_DURABLE", "CHILD_QUORUM_READY", "DELTA_REPLAY_PROGRESS", "FINAL_FENCE_DURABLE", "CHILDREN_REPLAYED_THROUGH_F", "META_CUTOVER_DURABLE", "CHILD_ACTIVATED", "PARENT_RETIRED"}
	if int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("SPLIT_STAGE_%d", s)
}

type SplitHook func(SplitHookStage, SplitRecord)

type metaMachine struct {
	state *metadataState
	hook  SplitHook
}

func (m *metaMachine) Apply(entry raft.Entry) error {
	next, err := decodeMetadata(entry.Command)
	if err != nil {
		return fmt.Errorf("decode committed metadata: %w", err)
	}
	if err := validateMetadataTransition(m.state, next); err != nil {
		return err
	}
	previous := m.state
	m.state = next
	if m.hook != nil {
		for id, record := range next.splits {
			old, existed := previous.splits[id]
			if !existed {
				m.hook(MetaSplitRecordDurable, cloneSplit(record))
			} else if old.State != record.State && record.State == SplitCommitted {
				m.hook(MetaCutoverDurable, cloneSplit(record))
			}
		}
	}
	return nil
}

func (m *metaMachine) Snapshot() ([]byte, error) { return encodeMetadata(m.state) }
func (m *metaMachine) Restore(data []byte) error {
	state, err := decodeMetadata(data)
	if err != nil {
		return err
	}
	m.state = state
	return nil
}

func validateMetadataTransition(old, next *metadataState) error {
	if old == nil || next == nil || next.nextRangeID < old.nextRangeID || next.nextSplitID < old.nextSplitID || next.nextReplicaID < old.nextReplicaID || next.nextMigrationID < old.nextMigrationID ||
		len(next.splits) < len(old.splits) || len(next.migrations) < len(old.migrations) || len(next.lineage) < len(old.lineage) {
		return ErrCorruptMetadata
	}
	controlChanged := !sameRebalanceControl(old.rebalance, next.rebalance)
	if controlChanged {
		if !sameMetadataCore(old, next) || !validRebalanceControlTransition(old.rebalance, next.rebalance) {
			return ErrCorruptMetadata
		}
		return nil
	}
	for parent, edge := range old.lineage {
		other, ok := next.lineage[parent]
		if !ok || edge.SplitID != other.SplitID || edge.Parent != other.Parent || edge.Children != other.Children ||
			edge.CutoverCatalogGeneration != other.CutoverCatalogGeneration || !bytes.Equal(edge.SplitKey, other.SplitKey) {
			return ErrCorruptMetadata
		}
	}
	changed := 0
	for id, before := range old.splits {
		after, ok := next.splits[id]
		if !ok {
			return ErrCorruptMetadata
		}
		if splitRecordsEqual(before, after) {
			continue
		}
		changed++
		if after.Epoch == before.Epoch+1 && after.State == before.State {
			continue
		}
		if after.Epoch != before.Epoch || !legalSplitTransition(before.State, after.State) {
			return ErrCorruptMetadata
		}
	}
	addedSplits := len(next.splits) - len(old.splits)
	for id, before := range old.migrations {
		after, ok := next.migrations[id]
		if !ok {
			return ErrCorruptMetadata
		}
		if before == after {
			continue
		}
		changed++
		if after.Epoch == before.Epoch+1 && after.State == before.State {
			continue
		}
		if after.Epoch != before.Epoch || !legalMigrationTransition(before.State, after.State) {
			return ErrCorruptMetadata
		}
	}
	addedMigrations := len(next.migrations) - len(old.migrations)
	if addedSplits+addedMigrations > 1 || changed+addedSplits+addedMigrations != 1 {
		return ErrCorruptMetadata
	}
	if addedSplits == 1 {
		if next.nextRangeID != old.nextRangeID+2 || next.nextSplitID != old.nextSplitID+1 ||
			next.nextReplicaID != old.nextReplicaID || next.nextMigrationID != old.nextMigrationID || next.catalog.Generation() != old.catalog.Generation() || len(next.lineage) != len(old.lineage) {
			return ErrCorruptMetadata
		}
		return nil
	}
	if addedMigrations == 1 {
		if next.nextReplicaID != old.nextReplicaID+1 || next.nextMigrationID != old.nextMigrationID+1 || next.nextRangeID != old.nextRangeID || next.nextSplitID != old.nextSplitID || next.catalog.Generation() != old.catalog.Generation() {
			return ErrCorruptMetadata
		}
		return nil
	}
	if next.nextRangeID != old.nextRangeID || next.nextSplitID != old.nextSplitID || next.nextReplicaID != old.nextReplicaID || next.nextMigrationID != old.nextMigrationID {
		return ErrCorruptMetadata
	}
	if next.catalog.Generation() == old.catalog.Generation()+1 {
		if len(next.lineage) != len(old.lineage)+1 && len(next.lineage) != len(old.lineage) {
			return ErrCorruptMetadata
		}
	} else if next.catalog.Fingerprint() != old.catalog.Fingerprint() || len(next.lineage) != len(old.lineage) {
		return ErrCorruptMetadata
	}
	return next.validateLineage()
}

func sameMetadataCore(left, right *metadataState) bool {
	if left.nextRangeID != right.nextRangeID || left.nextSplitID != right.nextSplitID || left.nextReplicaID != right.nextReplicaID || left.nextMigrationID != right.nextMigrationID ||
		left.catalog.Fingerprint() != right.catalog.Fingerprint() || len(left.splits) != len(right.splits) || len(left.migrations) != len(right.migrations) || len(left.lineage) != len(right.lineage) {
		return false
	}
	for id, record := range left.splits {
		if other, ok := right.splits[id]; !ok || !splitRecordsEqual(record, other) {
			return false
		}
	}
	for id, record := range left.migrations {
		if other, ok := right.migrations[id]; !ok || record != other {
			return false
		}
	}
	for id, record := range left.lineage {
		other, ok := right.lineage[id]
		if !ok || record.SplitID != other.SplitID || record.Parent != other.Parent || record.Children != other.Children || record.CutoverCatalogGeneration != other.CutoverCatalogGeneration || !bytes.Equal(record.SplitKey, other.SplitKey) {
			return false
		}
	}
	return true
}

func sameRebalanceControl(left, right RebalanceControlSnapshot) bool {
	if left.Policy != right.Policy || left.PolicyVersion != right.PolicyVersion || left.ControllerEpoch != right.ControllerEpoch || left.NextActionID != right.NextActionID || len(left.History) != len(right.History) || len(left.Cooldowns.Ranges) != len(right.Cooldowns.Ranges) || len(left.Cooldowns.Nodes) != len(right.Cooldowns.Nodes) {
		return false
	}
	for index := range left.History {
		if !sameRebalanceRecord(left.History[index], right.History[index]) {
			return false
		}
	}
	for id, deadline := range left.Cooldowns.Ranges {
		if other, ok := right.Cooldowns.Ranges[id]; !ok || !deadline.Equal(other) {
			return false
		}
	}
	for id, deadline := range left.Cooldowns.Nodes {
		if other, ok := right.Cooldowns.Nodes[id]; !ok || !deadline.Equal(other) {
			return false
		}
	}
	return true
}

func sameRebalanceRecord(left, right RebalanceActionRecord) bool {
	return sameRebalanceAction(left.Action, right.Action) && left.ControllerEpoch == right.ControllerEpoch && left.PolicyVersion == right.PolicyVersion && left.State == right.State &&
		left.PlannedAt.Equal(right.PlannedAt) && left.UpdatedAt.Equal(right.UpdatedAt) && left.MigrationID == right.MigrationID && left.SplitID == right.SplitID && left.LastError == right.LastError
}

func validRebalanceControlTransition(old, next RebalanceControlSnapshot) bool {
	if next.PolicyVersion != next.Policy.Version || next.PolicyVersion < old.PolicyVersion || next.ControllerEpoch < old.ControllerEpoch || next.ControllerEpoch > old.ControllerEpoch+1 || next.NextActionID < old.NextActionID || next.NextActionID > old.NextActionID+1 || len(next.History) > MaxRebalanceActionRecords {
		return false
	}
	if next.Policy != old.Policy && next.PolicyVersion <= old.PolicyVersion {
		return false
	}
	if next.ControllerEpoch == old.ControllerEpoch+1 && (next.PolicyVersion != old.PolicyVersion || next.NextActionID != old.NextActionID || len(next.History) != len(old.History)) {
		return false
	}
	if len(old.History) != 0 && len(next.History) == len(old.History) && next.NextActionID == old.NextActionID+1 {
		for index := 1; index < len(old.History); index++ {
			if !sameRebalanceRecord(old.History[index], next.History[index-1]) {
				return false
			}
		}
		record := next.History[len(next.History)-1]
		return record.Action.ActionID == old.NextActionID && record.State == RebalanceActionPlanned && record.ControllerEpoch == next.ControllerEpoch && record.PolicyVersion == next.PolicyVersion && cooldownsDoNotRegress(old.Cooldowns, next.Cooldowns)
	}
	if len(next.History) < len(old.History) || len(next.History) > len(old.History)+1 {
		return false
	}
	for index := 0; index < min(len(old.History), len(next.History)); index++ {
		if index == len(old.History)-1 && len(next.History) == len(old.History) {
			continue
		}
		if !sameRebalanceRecord(old.History[index], next.History[index]) {
			return false
		}
	}
	if len(next.History) == len(old.History)+1 {
		record := next.History[len(next.History)-1]
		if next.NextActionID != old.NextActionID+1 || record.Action.ActionID != old.NextActionID || record.State != RebalanceActionPlanned || record.ControllerEpoch != next.ControllerEpoch || record.PolicyVersion != next.PolicyVersion {
			return false
		}
	} else if len(old.History) != 0 && !sameRebalanceRecord(old.History[len(old.History)-1], next.History[len(next.History)-1]) {
		before, after := old.History[len(old.History)-1], next.History[len(next.History)-1]
		if before.Action.ActionID != after.Action.ActionID || before.ControllerEpoch != after.ControllerEpoch || before.PolicyVersion != after.PolicyVersion ||
			!sameRebalanceAction(before.Action, after.Action) || !legalRebalanceRecordUpdate(before, after) || after.UpdatedAt.Before(before.UpdatedAt) {
			return false
		}
	}
	return cooldownsDoNotRegress(old.Cooldowns, next.Cooldowns)
}

func legalRebalanceRecordUpdate(before, after RebalanceActionRecord) bool {
	if legalRebalanceActionTransition(before.State, after.State) {
		return true
	}
	return before.State == RebalanceActionExecuting && after.State == RebalanceActionExecuting &&
		(before.MigrationID == 0 || before.MigrationID == after.MigrationID) &&
		(before.SplitID == 0 || before.SplitID == after.SplitID)
}

func sameRebalanceAction(left, right RebalanceAction) bool {
	return left.ActionID == right.ActionID && left.Type == right.Type && left.Reason == right.Reason && left.RangeID == right.RangeID && left.RangeGeneration == right.RangeGeneration && left.CatalogGeneration == right.CatalogGeneration && left.SourceReplicaID == right.SourceReplicaID && left.SourceNodeID == right.SourceNodeID && left.TargetNodeID == right.TargetNodeID && bytes.Equal(left.SplitKey, right.SplitKey) && left.SourceScore == right.SourceScore && left.TargetScore == right.TargetScore && left.EstimatedCost == right.EstimatedCost && left.ExpectedImprovement == right.ExpectedImprovement && left.Emergency == right.Emergency && slices.Equal(left.Constraints, right.Constraints)
}

func legalRebalanceActionTransition(before, after RebalanceActionState) bool {
	return before == RebalanceActionPlanned && (after == RebalanceActionExecuting || after == RebalanceActionFailed) || before == RebalanceActionExecuting && (after == RebalanceActionSucceeded || after == RebalanceActionFailed)
}

func cooldownsDoNotRegress(old, next RebalanceCooldowns) bool {
	for id, deadline := range old.Ranges {
		if other, ok := next.Ranges[id]; !ok || other.Before(deadline) {
			return false
		}
	}
	for id, deadline := range old.Nodes {
		if other, ok := next.Nodes[id]; !ok || other.Before(deadline) {
			return false
		}
	}
	return true
}

func splitRecordsEqual(a, b SplitRecord) bool {
	return a.SplitID == b.SplitID && a.Parent == b.Parent && bytes.Equal(a.SplitKey, b.SplitKey) &&
		sameDescriptor(a.Left, b.Left) && sameDescriptor(a.Right, b.Right) && a.Epoch == b.Epoch && a.State == b.State &&
		a.BootstrapIndex == b.BootstrapIndex && a.FenceIndex == b.FenceIndex && a.LeftReplayThrough == b.LeftReplayThrough &&
		a.RightReplayThrough == b.RightReplayThrough && a.ImageDigest == b.ImageDigest && a.CutoverCatalogGeneration == b.CutoverCatalogGeneration && a.LastError == b.LastError
}

type MetaRangeOptions struct {
	Nodes     []raft.NodeID
	Directory string
	Bootstrap *Catalog
	Hook      SplitHook
	MaxWork   int
}

// MetaRange is a statically located independent Raft group. Its methods drive
// deterministic message delivery synchronously; it owns no timers or goroutine.
type MetaRange struct {
	mu       sync.Mutex
	nodes    map[raft.NodeID]*raft.Node
	machines map[raft.NodeID]*metaMachine
	queue    []raft.Message
	peers    []raft.NodeID
	maxWork  int
	stopped  map[raft.NodeID]bool
}

func OpenMetaRange(options MetaRangeOptions) (*MetaRange, error) {
	if len(options.Nodes) == 0 || options.Directory == "" || options.Bootstrap == nil {
		return nil, ErrInvalidCatalog
	}
	if options.MaxWork == 0 {
		options.MaxWork = 100_000
	}
	peers := append([]raft.NodeID(nil), options.Nodes...)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	for index, id := range peers {
		if id == 0 || index > 0 && id == peers[index-1] {
			return nil, ErrInvalidCatalog
		}
	}
	group := &MetaRange{nodes: make(map[raft.NodeID]*raft.Node), machines: make(map[raft.NodeID]*metaMachine), peers: peers, maxWork: options.MaxWork, stopped: make(map[raft.NodeID]bool)}
	for _, id := range peers {
		initial, err := newMetadataState(options.Bootstrap)
		if err != nil {
			return nil, err
		}
		machine := &metaMachine{state: initial, hook: options.Hook}
		store, err := raft.OpenFileStore(filepath.Join(options.Directory, fmt.Sprint(uint64(id)), "meta", "raft"))
		if err != nil {
			return nil, fmt.Errorf("open MetaRange store on node %d: %w", id, err)
		}
		node, err := raft.NewNode(raft.Config{ID: id, Peers: peers, ElectionTimeoutMin: 5 + uint64(id%3), ElectionTimeoutMax: 5 + uint64(id%3), HeartbeatInterval: 1,
			Random: rand.New(rand.NewPCG(uint64(id), uint64(MetaRangeID))), Store: store, StateMachine: machine}) //nolint:gosec // deterministic election input
		if err != nil {
			return nil, fmt.Errorf("open MetaRange replica on node %d: %w", id, err)
		}
		group.nodes[id], group.machines[id] = node, machine
	}
	return group, nil
}

func (m *MetaRange) leaderLocked() (raft.NodeID, bool) {
	for _, id := range m.peers {
		if m.stopped[id] {
			continue
		}
		if m.nodes[id].Status().Role == raft.Leader {
			return id, true
		}
	}
	return 0, false
}

func (m *MetaRange) driveLocked(until func() bool) error {
	for work := 0; work < m.maxWork; work++ {
		if until() {
			return nil
		}
		if len(m.queue) != 0 {
			message := m.queue[0]
			m.queue = m.queue[1:]
			if m.stopped[message.To] {
				continue
			}
			out, err := m.nodes[message.To].Step(message)
			if err != nil {
				return fmt.Errorf("deliver MetaRange message: %w", err)
			}
			m.queue = append(m.queue, out...)
			continue
		}
		for _, id := range m.peers {
			if m.stopped[id] {
				continue
			}
			out, err := m.nodes[id].Tick()
			if err != nil {
				return fmt.Errorf("tick MetaRange replica: %w", err)
			}
			m.queue = append(m.queue, out...)
		}
	}
	return ErrLeaderUnknown
}

func (m *MetaRange) mutate(ctx context.Context, change func(*metadataState) error) error {
	if ctx == nil {
		return ErrInvalidCatalog
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.driveLocked(func() bool { _, ok := m.leaderLocked(); return ok }); err != nil {
		return err
	}
	leader, _ := m.leaderLocked()
	next := cloneMetadata(m.machines[leader].state)
	if err := change(next); err != nil {
		return err
	}
	encoded, err := encodeMetadata(next)
	if err != nil {
		return err
	}
	current, err := encodeMetadata(m.machines[leader].state)
	if err != nil {
		return err
	}
	if bytes.Equal(current, encoded) {
		return nil
	}
	index, out, err := m.nodes[leader].Propose(encoded)
	if err != nil {
		return fmt.Errorf("propose MetaRange transition: %w", err)
	}
	m.queue = append(m.queue, out...)
	if err := m.driveLocked(func() bool {
		return m.nodes[leader].Status().LastApplied >= index || m.nodes[leader].Status().Role != raft.Leader
	}); err != nil {
		return err
	}
	if m.nodes[leader].Status().LastApplied < index {
		return raft.ErrNotLeader
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("complete metadata transition: %w", err)
	}
	return nil
}

func (m *MetaRange) Snapshot() MetadataSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	if leader, ok := m.leaderLocked(); ok {
		return m.machines[leader].state.snapshot()
	}
	return m.machines[m.peers[0]].state.snapshot()
}

func (m *MetaRange) SetRebalancePolicy(ctx context.Context, policy RebalancePolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	return m.mutate(ctx, func(state *metadataState) error {
		if policy.Version < state.rebalance.PolicyVersion {
			return ErrStaleRebalancePlan
		}
		if policy.Version == state.rebalance.PolicyVersion && policy != state.rebalance.Policy {
			return ErrStaleRebalancePlan
		}
		state.rebalance.Policy, state.rebalance.PolicyVersion = policy, policy.Version
		return nil
	})
}

func (m *MetaRange) SetRebalancePolicyVersion(ctx context.Context, version uint64) error {
	policy := DefaultRebalancePolicy()
	policy.Version = version
	return m.SetRebalancePolicy(ctx, policy)
}

func (m *MetaRange) TakeoverRebalanceController(ctx context.Context) (uint64, error) {
	var epoch uint64
	err := m.mutate(ctx, func(state *metadataState) error {
		if state.rebalance.ControllerEpoch == ^uint64(0) {
			return ErrResourceLimit
		}
		state.rebalance.ControllerEpoch++
		epoch = state.rebalance.ControllerEpoch
		return nil
	})
	return epoch, err
}

func (m *MetaRange) BeginRebalanceAction(ctx context.Context, action RebalanceAction, at time.Time) (RebalanceActionRecord, error) {
	return m.BeginRebalanceActionWithLimit(ctx, action, at, MaxRebalanceActionRecords)
}

func (m *MetaRange) BeginRebalanceActionWithLimit(ctx context.Context, action RebalanceAction, at time.Time, historyLimit uint32) (RebalanceActionRecord, error) {
	var result RebalanceActionRecord
	err := m.mutate(ctx, func(state *metadataState) error {
		control := &state.rebalance
		if control.PolicyVersion == 0 || control.ControllerEpoch == 0 || control.NextActionID == 0 || action.RangeID == 0 || action.Type < RebalanceMoveReplica || action.Type > RebalanceTransferLeader || historyLimit == 0 || historyLimit > MaxRebalanceActionRecords {
			return ErrResourceLimit
		}
		if uint32(len(control.History)) >= historyLimit { //nolint:gosec // history is bounded by MaxRebalanceActionRecords
			if control.History[0].State != RebalanceActionSucceeded && control.History[0].State != RebalanceActionFailed {
				return ErrResourceLimit
			}
			copy(control.History, control.History[1:])
			control.History = control.History[:len(control.History)-1]
		}
		action.ActionID = control.NextActionID
		result = RebalanceActionRecord{Action: cloneRebalanceAction(action), ControllerEpoch: control.ControllerEpoch, PolicyVersion: control.PolicyVersion, State: RebalanceActionPlanned, PlannedAt: at, UpdatedAt: at}
		control.History = append(control.History, result)
		control.NextActionID++
		return nil
	})
	return result, err
}

func (m *MetaRange) AdvanceRebalanceAction(ctx context.Context, actionID uint64, state RebalanceActionState, migrationID MigrationID, splitID SplitID, lastError string, at time.Time, rangeUntil, sourceUntil, targetUntil time.Time) (RebalanceActionRecord, error) {
	var result RebalanceActionRecord
	err := m.mutate(ctx, func(meta *metadataState) error {
		if len(meta.rebalance.History) == 0 {
			return ErrRangeNotFound
		}
		index := len(meta.rebalance.History) - 1
		record := meta.rebalance.History[index]
		if record.Action.ActionID != actionID {
			return ErrStaleRebalancePlan
		}
		if record.State == state {
			if migrationID != 0 {
				record.MigrationID = migrationID
			}
			if splitID != 0 {
				record.SplitID = splitID
			}
			if lastError != "" {
				record.LastError = lastError
			}
			if at.After(record.UpdatedAt) || at.Equal(record.UpdatedAt) {
				record.UpdatedAt = at
			}
			meta.rebalance.History[index] = record
			result = record
			return nil
		}
		if !legalRebalanceActionTransition(record.State, state) {
			return ErrStaleRebalancePlan
		}
		record.State, record.MigrationID, record.SplitID, record.LastError, record.UpdatedAt = state, migrationID, splitID, lastError, at
		meta.rebalance.History[index] = record
		if state == RebalanceActionSucceeded || state == RebalanceActionFailed {
			if !rangeUntil.IsZero() {
				meta.rebalance.Cooldowns.Ranges[record.Action.RangeID] = rangeUntil
			}
			if record.Action.SourceNodeID != 0 && !sourceUntil.IsZero() {
				meta.rebalance.Cooldowns.Nodes[record.Action.SourceNodeID] = sourceUntil
			}
			if record.Action.TargetNodeID != 0 && !targetUntil.IsZero() {
				meta.rebalance.Cooldowns.Nodes[record.Action.TargetNodeID] = targetUntil
			}
		}
		result = record
		return nil
	})
	return result, err
}

// Sync elects a metadata leader and causes retained committed history to be
// learned and applied after a full process restart.
func (m *MetaRange) Sync(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidCatalog
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.driveLocked(func() bool {
		leader, ok := m.leaderLocked()
		return ok && m.nodes[leader].Status().LastApplied == m.nodes[leader].Status().LastIndex
	}); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("synchronize metadata range: %w", err)
	}
	return nil
}

func (m *MetaRange) BeginSplit(ctx context.Context, parent RangeRef, key []byte, catalogGeneration uint64) (SplitRecord, error) {
	var result SplitRecord
	err := m.mutate(ctx, func(state *metadataState) error {
		var err error
		result, err = state.begin(parent, key, catalogGeneration)
		return err
	})
	return result, err
}

func (m *MetaRange) AdvanceSplit(ctx context.Context, id SplitID, epoch uint64, state SplitState, evidence SplitRecord) (SplitRecord, error) {
	var result SplitRecord
	err := m.mutate(ctx, func(meta *metadataState) error {
		var err error
		result, err = meta.advance(id, epoch, state, evidence)
		return err
	})
	return result, err
}

func (m *MetaRange) TakeoverSplit(ctx context.Context, id SplitID, epoch uint64) (SplitRecord, error) {
	var result SplitRecord
	err := m.mutate(ctx, func(meta *metadataState) error { var err error; result, err = meta.takeover(id, epoch); return err })
	return result, err
}

func (m *MetaRange) AbortSplit(ctx context.Context, id SplitID, epoch uint64, reason string) (SplitRecord, error) {
	return m.AdvanceSplit(ctx, id, epoch, SplitAborted, SplitRecord{LastError: reason})
}

func (m *MetaRange) CommitSplit(ctx context.Context, id SplitID, epoch, catalogGeneration uint64) (SplitRecord, error) {
	var result SplitRecord
	err := m.mutate(ctx, func(meta *metadataState) error {
		var err error
		result, err = meta.commit(id, epoch, catalogGeneration)
		return err
	})
	return result, err
}

func (m *MetaRange) BeginMigration(ctx context.Context, rangeID RangeID, source ReplicaID, target raft.NodeID, generation, catalogGeneration uint64) (MigrationRecord, error) {
	var result MigrationRecord
	err := m.mutate(ctx, func(state *metadataState) error {
		var e error
		result, e = state.beginMigration(rangeID, source, target, generation, catalogGeneration)
		return e
	})
	return result, err
}

func (m *MetaRange) AdvanceMigration(ctx context.Context, id MigrationID, epoch uint64, state MigrationState, evidence MigrationRecord) (MigrationRecord, error) {
	var result MigrationRecord
	err := m.mutate(ctx, func(meta *metadataState) error {
		var e error
		result, e = meta.advanceMigration(id, epoch, state, evidence)
		return e
	})
	return result, err
}

func (m *MetaRange) TakeoverMigration(ctx context.Context, id MigrationID, epoch uint64) (MigrationRecord, error) {
	var result MigrationRecord
	err := m.mutate(ctx, func(meta *metadataState) error { var e error; result, e = meta.takeoverMigration(id, epoch); return e })
	return result, err
}

func (m *MetaRange) CommitMigration(ctx context.Context, id MigrationID, epoch, catalogGeneration, configVersion uint64) (MigrationRecord, error) {
	var result MigrationRecord
	err := m.mutate(ctx, func(meta *metadataState) error {
		var e error
		result, e = meta.commitMigration(id, epoch, catalogGeneration, configVersion)
		return e
	})
	return result, err
}

func (m *MetaRange) ResolveRangeRef(ref RangeRef) ([]RangeDescriptor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	leader, ok := m.leaderLocked()
	if !ok {
		leader = m.peers[0]
	}
	return m.machines[leader].state.resolve(ref)
}

func (m *MetaRange) StopNode(id raft.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node := m.nodes[id]
	if node == nil {
		return ErrUnknownRange
	}
	node.Stop()
	m.stopped[id] = true
	return nil
}

func (m *MetaRange) Leader() raft.NodeID {
	m.mu.Lock()
	defer m.mu.Unlock()
	leader, _ := m.leaderLocked()
	return leader
}

var _ raft.StateMachine = (*metaMachine)(nil)
