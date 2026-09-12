package advisor

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/rivetdb/rivetdb/internal/multiraft"
)

type SnapshotInput struct {
	SnapshotVersion uint64
	Cluster         multiraft.RebalanceClusterSnapshot
	Control         multiraft.RebalanceControlSnapshot
	Performance     []PerformanceRecord
}

type PerformanceRecord struct {
	Category       string `json:"category"`
	Benchmark      string `json:"benchmark"`
	Scale          string `json:"scale"`
	Classification string `json:"classification"`
	MedianNS       uint64 `json:"median_ns"`
}

type AdvisorSnapshot struct {
	SchemaVersion                                                      uint32 `json:"schema_version"`
	SnapshotVersion, CatalogGeneration, PolicyVersion, ControllerEpoch uint64
	Nodes                                                              []NodeSummary       `json:"nodes"`
	Ranges                                                             []RangeSummary      `json:"ranges"`
	Cooldowns                                                          []CooldownSummary   `json:"cooldowns"`
	RecentActions                                                      []ActionSummary     `json:"recent_actions"`
	Health                                                             HealthSummary       `json:"health"`
	Performance                                                        []PerformanceRecord `json:"performance"`
}

type NodeSummary struct {
	NodeID                                               uint64 `json:"node_id"`
	HostedReplicas, Leaders, LogicalBytes, PhysicalBytes uint64
	ReadRate, WriteRate, RequestRate, ApplyBacklog       uint64
	MigrationsIn, MigrationsOut, SplitsInFlight          uint64
	Healthy, Available, Warming                          bool
}

type ReplicaSummary struct {
	ReplicaID, NodeID                        uint64
	Role                                     string
	Leader                                   bool
	MatchIndex, LastApplied, Lag, LocalBytes uint64
	Healthy                                  bool
}

type RangeSummary struct {
	RangeID, Generation, LogicalBytes, PhysicalBytes, KeyCount    uint64
	ReadRate, WriteRate, RequestRate                              uint64
	Samples                                                       uint32
	Leader, CommitIndex, LastApplied, ApplyBacklog, MVCCWatermark uint64
	Splitting, Migrating, Active                                  bool
	Replicas                                                      []ReplicaSummary `json:"replicas"`
}

type CooldownSummary struct {
	Kind          string
	ID            uint64
	UntilUnixNano int64
}
type ActionSummary struct {
	ActionID             uint64
	Type                 uint8
	Reason               string
	RangeID              uint64
	State                uint8
	MigrationID, SplitID uint64
	Failed               bool
}
type HealthSummary struct{ Nodes, HealthyNodes, AvailableNodes, Ranges, SplittingRanges, MigratingRanges, LaggingReplicas uint64 }

func BuildSnapshot(input SnapshotInput, cfg Config) (AdvisorSnapshot, []byte, [32]byte, error) {
	canonical, err := multiraft.CanonicalizeRebalanceSnapshot(input.Cluster)
	if err != nil || input.SnapshotVersion == 0 || canonical.CatalogGeneration == 0 ||
		canonical.PolicyVersion == 0 || canonical.ControllerEpoch == 0 ||
		input.Control.PolicyVersion != canonical.PolicyVersion || input.Control.ControllerEpoch != canonical.ControllerEpoch ||
		len(canonical.Nodes) > int(cfg.MaxNodes) || len(canonical.Ranges) > int(cfg.MaxRanges) {
		return AdvisorSnapshot{}, nil, [32]byte{}, ErrInvalidSnapshot
	}
	s := AdvisorSnapshot{SchemaVersion: SchemaVersion, SnapshotVersion: input.SnapshotVersion,
		CatalogGeneration: canonical.CatalogGeneration, PolicyVersion: canonical.PolicyVersion,
		ControllerEpoch: canonical.ControllerEpoch}
	for _, n := range canonical.Nodes {
		s.Nodes = append(s.Nodes, NodeSummary{NodeID: uint64(n.NodeID), HostedReplicas: n.HostedReplicas, Leaders: n.Leaders,
			LogicalBytes: n.LogicalBytes, PhysicalBytes: n.PhysicalBytes, ReadRate: n.ReadRate, WriteRate: n.WriteRate,
			RequestRate: n.RequestRate, ApplyBacklog: n.ApplyBacklog, MigrationsIn: n.MigrationsIn,
			MigrationsOut: n.MigrationsOut, SplitsInFlight: n.SplitsInFlight, Healthy: n.Healthy, Available: n.Available, Warming: n.Warming})
		s.Health.Nodes++
		if n.Healthy {
			s.Health.HealthyNodes++
		}
		if n.Available {
			s.Health.AvailableNodes++
		}
	}
	for _, r := range canonical.Ranges {
		rangeSummary := RangeSummary{RangeID: uint64(r.RangeID), Generation: r.Generation, LogicalBytes: r.LogicalBytes,
			PhysicalBytes: r.PhysicalBytes, KeyCount: r.KeyCount, ReadRate: r.ReadRate, WriteRate: r.WriteRate,
			RequestRate: r.RequestRate, Samples: r.Samples, Leader: uint64(r.Leader), CommitIndex: r.CommitIndex,
			LastApplied: r.LastApplied, ApplyBacklog: r.ApplyBacklog, MVCCWatermark: r.MVCCWatermark,
			Splitting: r.Splitting, Migrating: r.Migrating, Active: r.Active}
		for _, replica := range r.Replicas {
			if replica.Role != "VOTER" && replica.Role != "LEARNER" && replica.Role != "RETIRED" {
				return AdvisorSnapshot{}, nil, [32]byte{}, ErrInvalidSnapshot
			}
			rangeSummary.Replicas = append(rangeSummary.Replicas, ReplicaSummary{ReplicaID: uint64(replica.ReplicaID), NodeID: uint64(replica.NodeID), Role: replica.Role, Leader: replica.Leader, MatchIndex: replica.MatchIndex, LastApplied: replica.LastApplied, Lag: replica.Lag, LocalBytes: replica.LocalBytes, Healthy: replica.Healthy})
			if replica.Lag != 0 {
				s.Health.LaggingReplicas++
			}
		}
		s.Ranges = append(s.Ranges, rangeSummary)
		s.Health.Ranges++
		if r.Splitting {
			s.Health.SplittingRanges++
		}
		if r.Migrating {
			s.Health.MigratingRanges++
		}
	}
	for id, until := range input.Control.Cooldowns.Ranges {
		s.Cooldowns = append(s.Cooldowns, CooldownSummary{Kind: "RANGE", ID: uint64(id), UntilUnixNano: until.UnixNano()})
	}
	for id, until := range input.Control.Cooldowns.Nodes {
		s.Cooldowns = append(s.Cooldowns, CooldownSummary{Kind: "NODE", ID: uint64(id), UntilUnixNano: until.UnixNano()})
	}
	sort.Slice(s.Cooldowns, func(i, j int) bool {
		if s.Cooldowns[i].Kind != s.Cooldowns[j].Kind {
			return s.Cooldowns[i].Kind < s.Cooldowns[j].Kind
		}
		return s.Cooldowns[i].ID < s.Cooldowns[j].ID
	})
	history := append([]multiraft.RebalanceActionRecord(nil), input.Control.History...)
	sort.Slice(history, func(i, j int) bool { return history[i].Action.ActionID < history[j].Action.ActionID })
	start := 0
	if len(history) > int(cfg.MaxRecentActions) {
		start = len(history) - int(cfg.MaxRecentActions)
	}
	for _, record := range history[start:] {
		if !validReason(record.Action.Reason) {
			return AdvisorSnapshot{}, nil, [32]byte{}, ErrInvalidSnapshot
		}
		s.RecentActions = append(s.RecentActions, ActionSummary{ActionID: record.Action.ActionID, Type: uint8(record.Action.Type), Reason: string(record.Action.Reason), RangeID: uint64(record.Action.RangeID), State: uint8(record.State), MigrationID: uint64(record.MigrationID), SplitID: uint64(record.SplitID), Failed: record.State == multiraft.RebalanceActionFailed})
	}
	if len(input.Performance) > int(cfg.MaxPerformanceRecords) {
		return AdvisorSnapshot{}, nil, [32]byte{}, ErrInvalidSnapshot
	}
	for _, p := range input.Performance {
		if !validPerformance(p) {
			return AdvisorSnapshot{}, nil, [32]byte{}, fmt.Errorf("%w: performance classification", ErrInvalidSnapshot)
		}
		s.Performance = append(s.Performance, p)
	}
	sort.Slice(s.Performance, func(i, j int) bool {
		if s.Performance[i].Category != s.Performance[j].Category {
			return s.Performance[i].Category < s.Performance[j].Category
		}
		if s.Performance[i].Benchmark != s.Performance[j].Benchmark {
			return s.Performance[i].Benchmark < s.Performance[j].Benchmark
		}
		return s.Performance[i].Scale < s.Performance[j].Scale
	})
	data, err := json.Marshal(s)
	if err != nil {
		return AdvisorSnapshot{}, nil, [32]byte{}, fmt.Errorf("marshal advisor snapshot: %w", err)
	}
	if len(data) > int(cfg.MaxInputBytes) {
		return AdvisorSnapshot{}, nil, [32]byte{}, ErrInputTooLarge
	}
	return s, data, sha256.Sum256(data), nil
}

func validPerformance(p PerformanceRecord) bool {
	if !boundedNonempty(p.Category, 128) || !boundedNonempty(p.Benchmark, 128) || !boundedNonempty(p.Scale, 128) {
		return false
	}
	switch p.Classification {
	case "CONSTRAINED-ENVIRONMENT", "IN-PROCESS", "EXACT-HOST CPU", "NOT MEASURED":
		return true
	}
	return false
}

func validReason(reason multiraft.RebalanceReason) bool {
	switch reason {
	case multiraft.ReasonNodeOverloaded, multiraft.ReasonRangeTooLarge, multiraft.ReasonRangeTooHot,
		multiraft.ReasonLeaderSkew, multiraft.ReasonReplicaCountSkew, multiraft.ReasonCapacityPressure,
		multiraft.ReasonBalanced, multiraft.ReasonCooldown, multiraft.ReasonNoEligibleTarget,
		multiraft.ReasonOperationInProgress, multiraft.ReasonInsufficientSamples,
		multiraft.ReasonBenefitBelowThreshold, multiraft.ReasonHotUnsplittableKeyspace:
		return true
	}
	return false
}
