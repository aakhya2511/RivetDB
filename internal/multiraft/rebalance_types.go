package multiraft

import (
	"bytes"
	"errors"
	"time"

	"github.com/rivetdb/rivetdb/internal/raft"
)

var (
	ErrInvalidRebalancePolicy = errors.New("multiraft: invalid rebalance policy")
	ErrStaleRebalancePlan     = errors.New("multiraft: stale rebalance plan")
	ErrRebalanceDisabled      = errors.New("multiraft: automatic rebalancing disabled")
	ErrNoEligibleAction       = errors.New("multiraft: no eligible rebalance action")
	ErrInvalidTelemetry       = errors.New("multiraft: invalid rebalance telemetry")
)

const rateScale = uint64(1_000_000)

const MaxRebalanceActionRecords = 4096

type RebalancePolicy struct {
	Version                            uint64
	Enabled, Moves, Splits, Leaders    bool
	SampleInterval                     time.Duration
	EWMAAlphaPPM                       uint64
	MinSamples                         uint32
	NodeImbalanceStartPPM              uint64
	NodeImbalanceRecoveryPPM           uint64
	BytesWeight                        uint64
	WriteWeight                        uint64
	ReadWeight                         uint64
	LeaderWeight                       uint64
	BacklogWeight                      uint64
	ReplicaWeight                      uint64
	RangeBytesWeight                   uint64
	RangeWriteWeight                   uint64
	RangeReadWeight                    uint64
	RangeBacklogWeight                 uint64
	MaxLogicalBytesPerNode             uint64
	MaxReplicaCountPerNode             uint64
	MaxLeaderCountPerNode              uint64
	MaxWriteRatePerNode                uint64
	RangeSplitBytes                    uint64
	RangeHotReadRate                   uint64
	RangeHotWriteRate                  uint64
	MinRangeBytes                      uint64
	MaxRanges                          uint64
	MinExpectedImprovement             uint64
	MaxActionsPerCycle                 uint32
	MaxConcurrentMigrationsCluster     uint32
	MaxConcurrentSplitsCluster         uint32
	MaxOutgoingMigrationsPerNode       uint32
	MaxIncomingMigrationsPerNode       uint32
	MaxSplitsPerNode                   uint32
	Cooldown                           time.Duration
	SplitCooldown                      time.Duration
	FailureCooldown                    time.Duration
	HistoryLimit                       uint32
	EmergencyCapacityOverridesCooldown bool
}

func DefaultRebalancePolicy() RebalancePolicy {
	return RebalancePolicy{Version: 1, Enabled: true, Moves: true, Splits: true, Leaders: true,
		SampleInterval: time.Second, EWMAAlphaPPM: 250_000, MinSamples: 3,
		NodeImbalanceStartPPM: 200_000, NodeImbalanceRecoveryPPM: 100_000,
		BytesWeight: 4, WriteWeight: 3, ReadWeight: 2, LeaderWeight: 2, BacklogWeight: 2, ReplicaWeight: 1,
		RangeBytesWeight: 4, RangeWriteWeight: 3, RangeReadWeight: 2, RangeBacklogWeight: 2,
		RangeSplitBytes: 64 << 20, RangeHotReadRate: 10_000, RangeHotWriteRate: 5_000, MinRangeBytes: 1 << 20, MaxRanges: MaxCatalogRanges,
		MinExpectedImprovement: 1, MaxActionsPerCycle: 1, MaxConcurrentMigrationsCluster: 2, MaxConcurrentSplitsCluster: 1,
		MaxOutgoingMigrationsPerNode: 1, MaxIncomingMigrationsPerNode: 1, MaxSplitsPerNode: 1,
		Cooldown: 5 * time.Minute, SplitCooldown: 10 * time.Minute, FailureCooldown: time.Minute, HistoryLimit: 1024}
}

func (p RebalancePolicy) Validate() error {
	weights := p.BytesWeight + p.WriteWeight + p.ReadWeight + p.LeaderWeight + p.BacklogWeight + p.ReplicaWeight
	rangeWeights := p.RangeBytesWeight + p.RangeWriteWeight + p.RangeReadWeight + p.RangeBacklogWeight
	if p.Version == 0 || p.SampleInterval <= 0 || p.EWMAAlphaPPM == 0 || p.EWMAAlphaPPM > rateScale || p.MinSamples == 0 ||
		p.NodeImbalanceStartPPM <= p.NodeImbalanceRecoveryPPM || weights == 0 || rangeWeights == 0 || p.MinRangeBytes == 0 ||
		p.RangeSplitBytes < p.MinRangeBytes || p.MaxRanges == 0 || p.MaxActionsPerCycle == 0 || p.MaxConcurrentMigrationsCluster == 0 ||
		p.MaxConcurrentSplitsCluster == 0 || p.MaxOutgoingMigrationsPerNode == 0 || p.MaxIncomingMigrationsPerNode == 0 || p.MaxSplitsPerNode == 0 ||
		p.Cooldown < 0 || p.SplitCooldown < 0 || p.FailureCooldown < 0 || p.HistoryLimit == 0 {
		return ErrInvalidRebalancePolicy
	}
	return nil
}

type RebalanceReplicaMetric struct {
	RangeID     RangeID
	ReplicaID   ReplicaID
	NodeID      raft.NodeID
	Role        string
	Leader      bool
	MatchIndex  uint64
	LastApplied uint64
	Lag         uint64
	LocalBytes  uint64
	Healthy     bool
}

type RebalanceRangeMetric struct {
	RangeID                                RangeID
	Generation                             uint64
	StartKey, EndKey                       []byte
	StartUnbounded, EndUnbounded           bool
	LogicalBytes, PhysicalBytes, KeyCount  uint64
	UserKeys                               [][]byte
	ReadRate, WriteRate, RequestRate       uint64
	Samples                                uint32
	Leader                                 raft.NodeID
	CommitIndex, LastApplied, ApplyBacklog uint64
	MVCCWatermark                          uint64
	Splitting, Migrating, Active           bool
	Replicas                               []RebalanceReplicaMetric
}

type RebalanceNodeMetric struct {
	NodeID                                                     raft.NodeID
	HostedReplicas, Leaders, LogicalBytes, PhysicalBytes       uint64
	ReadRate, WriteRate, RequestRate, ApplyBacklog             uint64
	MigrationLoad, MigrationsIn, MigrationsOut, SplitsInFlight uint64
	Healthy, Available, Warming                                bool
}

type RebalanceClusterSnapshot struct {
	SnapshotTime      time.Time
	CatalogGeneration uint64
	PolicyVersion     uint64
	ControllerEpoch   uint64
	Nodes             []RebalanceNodeMetric
	Ranges            []RebalanceRangeMetric
}

type RebalanceActionType uint8

const (
	RebalanceMoveReplica RebalanceActionType = iota + 1
	RebalanceSplitRange
	RebalanceTransferLeader
	RebalanceNoop
)

type RebalanceReason string

const (
	ReasonNodeOverloaded          RebalanceReason = "NODE_OVERLOADED"
	ReasonRangeTooLarge           RebalanceReason = "RANGE_TOO_LARGE"
	ReasonRangeTooHot             RebalanceReason = "RANGE_TOO_HOT"
	ReasonLeaderSkew              RebalanceReason = "LEADER_SKEW"
	ReasonReplicaCountSkew        RebalanceReason = "REPLICA_COUNT_SKEW"
	ReasonCapacityPressure        RebalanceReason = "CAPACITY_PRESSURE"
	ReasonBalanced                RebalanceReason = "BALANCED"
	ReasonCooldown                RebalanceReason = "COOLDOWN"
	ReasonNoEligibleTarget        RebalanceReason = "NO_ELIGIBLE_TARGET"
	ReasonOperationInProgress     RebalanceReason = "OPERATION_IN_PROGRESS"
	ReasonInsufficientSamples     RebalanceReason = "INSUFFICIENT_SAMPLES"
	ReasonBenefitBelowThreshold   RebalanceReason = "BENEFIT_BELOW_THRESHOLD"
	ReasonHotUnsplittableKeyspace RebalanceReason = "HOT_UNSPLITTABLE_KEYSPACE"
)

type RebalanceAction struct {
	ActionID                                uint64
	Type                                    RebalanceActionType
	Reason                                  RebalanceReason
	RangeID                                 RangeID
	RangeGeneration, CatalogGeneration      uint64
	SourceReplicaID                         ReplicaID
	SourceNodeID, TargetNodeID              raft.NodeID
	SplitKey                                []byte
	SourceScore, TargetScore, EstimatedCost uint64
	ExpectedImprovement                     uint64
	Constraints                             []string
	Emergency                               bool
}

type RebalancePlan struct {
	SnapshotGeneration uint64
	PolicyVersion      uint64
	ControllerEpoch    uint64
	Actions            []RebalanceAction
	NoopReason         RebalanceReason
}

// RebalanceCooldowns is replicated controller memory supplied to the pure
// planner. Times are absolute so restart does not silently forgive churn.
type RebalanceCooldowns struct {
	Ranges map[RangeID]time.Time
	Nodes  map[raft.NodeID]time.Time
}

type RebalanceActionState uint8

const (
	RebalanceActionPlanned RebalanceActionState = iota + 1
	RebalanceActionExecuting
	RebalanceActionSucceeded
	RebalanceActionFailed
)

func (s RebalanceActionState) valid() bool {
	return s >= RebalanceActionPlanned && s <= RebalanceActionFailed
}

type RebalanceActionRecord struct {
	Action          RebalanceAction
	ControllerEpoch uint64
	PolicyVersion   uint64
	State           RebalanceActionState
	PlannedAt       time.Time
	UpdatedAt       time.Time
	MigrationID     MigrationID
	SplitID         SplitID
	LastError       string
}

type RebalanceControlSnapshot struct {
	Policy          RebalancePolicy
	PolicyVersion   uint64
	ControllerEpoch uint64
	NextActionID    uint64
	History         []RebalanceActionRecord
	Cooldowns       RebalanceCooldowns
}

func cloneRebalanceAction(action RebalanceAction) RebalanceAction {
	action.SplitKey = bytes.Clone(action.SplitKey)
	action.Constraints = append([]string(nil), action.Constraints...)
	return action
}

func cloneRebalanceControl(source RebalanceControlSnapshot) RebalanceControlSnapshot {
	result := source
	result.History = make([]RebalanceActionRecord, len(source.History))
	for index, record := range source.History {
		record.Action = cloneRebalanceAction(record.Action)
		result.History[index] = record
	}
	result.Cooldowns.Ranges = make(map[RangeID]time.Time, len(source.Cooldowns.Ranges))
	for id, deadline := range source.Cooldowns.Ranges {
		result.Cooldowns.Ranges[id] = deadline
	}
	result.Cooldowns.Nodes = make(map[raft.NodeID]time.Time, len(source.Cooldowns.Nodes))
	for id, deadline := range source.Cooldowns.Nodes {
		result.Cooldowns.Nodes[id] = deadline
	}
	return result
}
