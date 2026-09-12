// Package advisor implements the optional, read-only Phase 12 AI advisory
// boundary. It deliberately exposes no database or controller mutation API.
package advisor

import (
	"context"
	"errors"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/multiraft"
)

const (
	SchemaVersion = uint32(1)
	PromptVersion = "rivetdb-advisor-v1"
)

var (
	ErrDisabled            = errors.New("advisor: disabled")
	ErrInvalidConfig       = errors.New("advisor: invalid config")
	ErrInvalidSnapshot     = errors.New("advisor: invalid snapshot")
	ErrInputTooLarge       = errors.New("advisor: input too large")
	ErrOutputTooLarge      = errors.New("advisor: output too large")
	ErrMalformedAdvice     = errors.New("advisor: malformed advice")
	ErrStaleAdvice         = errors.New("advisor: stale advice")
	ErrUnknownIdentity     = errors.New("advisor: unknown identity")
	ErrConflictingAdvice   = errors.New("advisor: conflicting advice")
	ErrRejectedByValidator = errors.New("advisor: rejected by deterministic validator")
	ErrAdvisorTimeout      = errors.New("advisor: request timed out")
	ErrModelFailure        = errors.New("advisor: model failure")
	ErrBusy                = errors.New("advisor: request capacity reached")
	ErrRateLimited         = errors.New("advisor: minimum advice interval not elapsed")
	ErrAuditLink           = errors.New("advisor: invalid audit linkage")
)

type Config struct {
	Version, MaxNodes, MaxRanges, MaxRecentActions, MaxPerformanceRecords uint32
	MaxInputBytes, MaxOutputBytes, MaxExplanationBytes                    uint32
	MaxActions, MaxFindings                                               uint32
	Enabled, RequireHumanApproval                                         bool
	Provider, Model                                                       string
	Timeout, MinAdviceInterval                                            time.Duration
	MaxConcurrentAdviceRequests                                           uint32
	Clock                                                                 clock.Clock
}

func DefaultConfig(c clock.Clock) Config {
	return Config{Version: 1, MaxNodes: 256, MaxRanges: multiraft.MaxCatalogRanges,
		MaxRecentActions: 128, MaxPerformanceRecords: 128, MaxInputBytes: 4 << 20,
		MaxOutputBytes: 256 << 10, MaxExplanationBytes: 4096, MaxActions: 16,
		MaxFindings: 32, MaxConcurrentAdviceRequests: 1, RequireHumanApproval: true, Timeout: 30 * time.Second, Clock: c}
}

func (c Config) validate() error {
	if c.Version != 1 || c.MaxNodes == 0 || c.MaxRanges == 0 || c.MaxRecentActions == 0 ||
		c.MaxPerformanceRecords == 0 || c.MaxInputBytes == 0 || c.MaxOutputBytes == 0 ||
		c.MaxExplanationBytes == 0 || c.MaxActions == 0 || c.MaxFindings == 0 ||
		c.MaxConcurrentAdviceRequests == 0 || !c.RequireHumanApproval || c.Timeout <= 0 || c.MinAdviceInterval < 0 || c.Clock == nil {
		return ErrInvalidConfig
	}
	return nil
}

type AdvisorModel interface {
	Analyze(context.Context, []byte) ([]byte, error)
}

type ActionType string

const (
	ActionNoop           ActionType = "NOOP"
	ActionInvestigate    ActionType = "INVESTIGATE"
	ActionWait           ActionType = "WAIT"
	ActionMove           ActionType = "MOVE"
	ActionSplit          ActionType = "SPLIT"
	ActionTransferLeader ActionType = "TRANSFER_LEADER"
)

type Diagnosis string

const (
	DiagnosisNodeOverload     Diagnosis = "NODE_OVERLOAD"
	DiagnosisLeaderSkew       Diagnosis = "LEADER_SKEW"
	DiagnosisHotRange         Diagnosis = "HOT_RANGE"
	DiagnosisLargeRange       Diagnosis = "LARGE_RANGE"
	DiagnosisFollowerLag      Diagnosis = "FOLLOWER_LAG"
	DiagnosisMigrationStalled Diagnosis = "MIGRATION_STALLED"
	DiagnosisSplitStalled     Diagnosis = "SPLIT_STALLED"
	DiagnosisTxnConflict      Diagnosis = "TRANSACTION_CONFLICT_PRESSURE"
	DiagnosisMetaUnavailable  Diagnosis = "METARANGE_UNAVAILABLE"
	DiagnosisNoSafeAction     Diagnosis = "NO_SAFE_ACTION"
	DiagnosisInsufficientData Diagnosis = "INSUFFICIENT_DATA"
)

type Confidence string

const (
	ConfidenceHigh   Confidence = "HIGH"
	ConfidenceMedium Confidence = "MEDIUM"
	ConfidenceLow    Confidence = "LOW"
)

type Evidence struct {
	Field string `json:"field"`
	Value uint64 `json:"value"`
	Unit  string `json:"unit"`
}

type Finding struct {
	Category Diagnosis  `json:"category"`
	Summary  string     `json:"summary"`
	Evidence []Evidence `json:"evidence"`
}

type ProposedAction struct {
	Type        ActionType `json:"type"`
	RangeID     uint64     `json:"range_id,omitempty"`
	Reason      string     `json:"reason"`
	Benefit     string     `json:"benefit"`
	Cost        string     `json:"cost"`
	Constraints []string   `json:"constraints"`
	Evidence    []Evidence `json:"evidence"`
	Confidence  Confidence `json:"confidence"`
}

type AdviceStatus string

const (
	StatusGenerated         AdviceStatus = "GENERATED"
	StatusInvalid           AdviceStatus = "INVALID"
	StatusStale             AdviceStatus = "STALE"
	StatusRejectedValidator AdviceStatus = "REJECTED_BY_VALIDATOR"
	StatusApprovedReview    AdviceStatus = "APPROVED_FOR_REVIEW"
)

type Advice struct {
	AdviceID          uint64           `json:"-"`
	SnapshotVersion   uint64           `json:"snapshot_version"`
	CatalogGeneration uint64           `json:"catalog_generation"`
	PolicyVersion     uint64           `json:"policy_version"`
	Summary           string           `json:"summary"`
	Findings          []Finding        `json:"findings"`
	Actions           []ProposedAction `json:"actions"`
	Confidence        Confidence       `json:"confidence"`
	Limitations       []string         `json:"limitations"`
	Status            AdviceStatus     `json:"-"`
	SnapshotDigest    [32]byte         `json:"-"`
}

type ValidationOutcome struct {
	Index  int
	Status AdviceStatus
	Reason string
	Action multiraft.RebalanceAction
}

type Approval struct {
	AdviceID    uint64
	ActionIndex int
	Operator    string
}

type AdviceRecord struct {
	AdviceID                         uint64
	At                               time.Time
	SnapshotDigest                   [32]byte
	CatalogGeneration, PolicyVersion uint64
	Provider, Model, PromptVersion   string
	SchemaVersion                    uint32
	Advice                           Advice
	ValidatorOutcomes                []ValidationOutcome
	ApprovedBy                       string
	ActionID                         uint64
	MigrationID                      multiraft.MigrationID
	SplitID                          multiraft.SplitID
}

type Counters struct {
	Generated, Valid, Invalid, Stale, ValidatorAccepted, ValidatorRejected          uint64
	UnknownIDs, Conflicts, Timeouts, ModelFailures, LinkedActions, SafetyViolations uint64
	PlannerAgreement, DifferentValidAction, AdvisorNoop                             uint64
}
