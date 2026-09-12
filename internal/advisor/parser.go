package advisor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

func parseAdvice(raw []byte, snapshot AdvisorSnapshot, digest [32]byte, cfg Config) (Advice, error) {
	if len(raw) > int(cfg.MaxOutputBytes) {
		return Advice{}, ErrOutputTooLarge
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var advice Advice
	if err := decoder.Decode(&advice); err != nil {
		return Advice{}, fmt.Errorf("%w: %w", ErrMalformedAdvice, err)
	}
	if err := requireEOF(decoder); err != nil {
		return Advice{}, err
	}
	if advice.SnapshotVersion != snapshot.SnapshotVersion || advice.CatalogGeneration != snapshot.CatalogGeneration || advice.PolicyVersion != snapshot.PolicyVersion {
		return Advice{}, ErrStaleAdvice
	}
	if len(advice.Findings) > int(cfg.MaxFindings) || len(advice.Actions) > int(cfg.MaxActions) || len(advice.Limitations) > 32 || !boundedNonempty(advice.Summary, cfg.MaxExplanationBytes) || !validConfidence(advice.Confidence) {
		return Advice{}, ErrMalformedAdvice
	}
	values := evidenceValues(snapshot)
	for _, finding := range advice.Findings {
		if !validDiagnosis(finding.Category) || !boundedNonempty(finding.Summary, cfg.MaxExplanationBytes) || len(finding.Evidence) == 0 || len(finding.Evidence) > 32 {
			return Advice{}, ErrMalformedAdvice
		}
		for _, evidence := range finding.Evidence {
			value, ok := values[evidence.Field]
			if !ok || value != evidence.Value || !boundedNonempty(evidence.Unit, 64) {
				return Advice{}, ErrMalformedAdvice
			}
		}
	}
	seen := make(map[string]struct{}, len(advice.Actions))
	rangeKinds := make(map[uint64]ActionType)
	for _, action := range advice.Actions {
		if !validAction(action.Type) || !validConfidence(action.Confidence) || !boundedNonempty(action.Reason, cfg.MaxExplanationBytes) || !boundedNonempty(action.Benefit, cfg.MaxExplanationBytes) || !boundedNonempty(action.Cost, cfg.MaxExplanationBytes) || len(action.Constraints) > 32 || len(action.Evidence) == 0 || len(action.Evidence) > 32 {
			return Advice{}, ErrMalformedAdvice
		}
		for _, evidence := range action.Evidence {
			value, ok := values[evidence.Field]
			if !ok || value != evidence.Value || !boundedNonempty(evidence.Unit, 64) {
				return Advice{}, ErrMalformedAdvice
			}
		}
		executable := action.Type == ActionMove || action.Type == ActionSplit || action.Type == ActionTransferLeader
		if executable != (action.RangeID != 0) {
			return Advice{}, ErrMalformedAdvice
		}
		for _, constraint := range action.Constraints {
			if !boundedNonempty(constraint, cfg.MaxExplanationBytes) {
				return Advice{}, ErrMalformedAdvice
			}
		}
		key := string(action.Type) + ":" + strconv.FormatUint(action.RangeID, 10)
		if _, exists := seen[key]; exists {
			return Advice{}, ErrConflictingAdvice
		}
		seen[key] = struct{}{}
		if executable {
			if prior, exists := rangeKinds[action.RangeID]; exists && prior != action.Type {
				return Advice{}, ErrConflictingAdvice
			}
			rangeKinds[action.RangeID] = action.Type
		}
	}
	for _, limitation := range advice.Limitations {
		if !boundedNonempty(limitation, cfg.MaxExplanationBytes) {
			return Advice{}, ErrMalformedAdvice
		}
	}
	advice.Status, advice.SnapshotDigest = StatusGenerated, digest
	return advice, nil
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrMalformedAdvice
	}
	return nil
}
func bounded(value string, limit uint32) bool         { return len(value) <= int(limit) }
func boundedNonempty(value string, limit uint32) bool { return value != "" && bounded(value, limit) }
func validConfidence(c Confidence) bool {
	return c == ConfidenceHigh || c == ConfidenceMedium || c == ConfidenceLow
}
func validAction(a ActionType) bool {
	return a == ActionNoop || a == ActionInvestigate || a == ActionWait || a == ActionMove || a == ActionSplit || a == ActionTransferLeader
}
func validDiagnosis(d Diagnosis) bool {
	switch d {
	case DiagnosisNodeOverload, DiagnosisLeaderSkew, DiagnosisHotRange, DiagnosisLargeRange, DiagnosisFollowerLag, DiagnosisMigrationStalled, DiagnosisSplitStalled, DiagnosisTxnConflict, DiagnosisMetaUnavailable, DiagnosisNoSafeAction, DiagnosisInsufficientData:
		return true
	}
	return false
}

func evidenceValues(s AdvisorSnapshot) map[string]uint64 {
	values := map[string]uint64{"health.nodes": s.Health.Nodes, "health.healthy_nodes": s.Health.HealthyNodes, "health.available_nodes": s.Health.AvailableNodes, "health.ranges": s.Health.Ranges, "health.splitting_ranges": s.Health.SplittingRanges, "health.migrating_ranges": s.Health.MigratingRanges, "health.lagging_replicas": s.Health.LaggingReplicas}
	for _, n := range s.Nodes {
		prefix := "nodes." + strconv.FormatUint(n.NodeID, 10) + "."
		values[prefix+"write_rate"] = n.WriteRate
		values[prefix+"read_rate"] = n.ReadRate
		values[prefix+"leaders"] = n.Leaders
		values[prefix+"logical_bytes"] = n.LogicalBytes
		values[prefix+"apply_backlog"] = n.ApplyBacklog
		values[prefix+"hosted_replicas"] = n.HostedReplicas
	}
	for _, r := range s.Ranges {
		prefix := "ranges." + strconv.FormatUint(r.RangeID, 10) + "."
		values[prefix+"write_rate"] = r.WriteRate
		values[prefix+"read_rate"] = r.ReadRate
		values[prefix+"logical_bytes"] = r.LogicalBytes
		values[prefix+"physical_bytes"] = r.PhysicalBytes
		values[prefix+"apply_backlog"] = r.ApplyBacklog
		values[prefix+"samples"] = uint64(r.Samples)
	}
	return values
}
