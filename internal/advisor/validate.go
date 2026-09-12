package advisor

import (
	"errors"
	"fmt"

	"github.com/rivetdb/rivetdb/internal/multiraft"
)

func (s *Service) Approve(approval Approval, basis SnapshotInput, fresh multiraft.RebalanceClusterSnapshot, policy multiraft.RebalancePolicy, cooldowns multiraft.RebalanceCooldowns) (ValidationOutcome, error) {
	s.mu.Lock()
	record, ok := s.records[approval.AdviceID]
	s.mu.Unlock()
	if !ok || approval.Operator == "" || approval.ActionIndex < 0 || approval.ActionIndex >= len(record.Advice.Actions) {
		return ValidationOutcome{}, ErrAuditLink
	}
	advice := record.Advice
	proposed := advice.Actions[approval.ActionIndex]
	_, _, basisDigest, basisErr := BuildSnapshot(basis, s.cfg)
	if advice.SnapshotVersion != basis.SnapshotVersion || advice.CatalogGeneration != basis.Cluster.CatalogGeneration || advice.PolicyVersion != basis.Cluster.PolicyVersion ||
		basisErr != nil || basisDigest != record.SnapshotDigest || fresh.CatalogGeneration != advice.CatalogGeneration || fresh.PolicyVersion != advice.PolicyVersion || fresh.ControllerEpoch != basis.Cluster.ControllerEpoch {
		out := ValidationOutcome{Index: approval.ActionIndex, Status: StatusStale, Reason: ErrStaleAdvice.Error()}
		s.recordOutcome(approval.AdviceID, approval.Operator, out)
		s.increment(func(c *Counters) { c.Stale++ })
		return out, ErrStaleAdvice
	}
	wanted, executable := rebalanceType(proposed.Type)
	if !executable {
		out := ValidationOutcome{Index: approval.ActionIndex, Status: StatusApprovedReview, Reason: "advisory-only recommendation"}
		s.recordOutcome(approval.AdviceID, approval.Operator, out)
		return out, nil
	}
	var exists, active bool
	for _, r := range fresh.Ranges {
		if uint64(r.RangeID) == proposed.RangeID {
			exists = true
			active = r.Active
			break
		}
	}
	if !exists || !active {
		out := ValidationOutcome{Index: approval.ActionIndex, Status: StatusRejectedValidator, Reason: ErrUnknownIdentity.Error()}
		s.recordOutcome(approval.AdviceID, approval.Operator, out)
		s.increment(func(c *Counters) { c.UnknownIDs++; c.ValidatorRejected++ })
		return out, ErrUnknownIdentity
	}
	plan, err := multiraft.PlanRebalance(fresh, policy, cooldowns)
	if err != nil {
		return s.reject(approval, err)
	}
	var selected multiraft.RebalanceAction
	for _, action := range plan.Actions {
		if action.Type == wanted && uint64(action.RangeID) == proposed.RangeID {
			selected = action
			break
		}
	}
	if selected.RangeID == 0 {
		return s.reject(approval, ErrRejectedByValidator)
	}
	candidate := multiraft.RebalancePlan{SnapshotGeneration: fresh.CatalogGeneration, PolicyVersion: fresh.PolicyVersion, ControllerEpoch: fresh.ControllerEpoch, Actions: []multiraft.RebalanceAction{selected}}
	if err := multiraft.ValidateRebalancePlan(candidate, fresh, policy); err != nil {
		return s.reject(approval, err)
	}
	out := ValidationOutcome{Index: approval.ActionIndex, Status: StatusApprovedReview, Reason: "fresh Phase 9 validation passed", Action: selected}
	s.recordOutcome(approval.AdviceID, approval.Operator, out)
	s.increment(func(c *Counters) { c.ValidatorAccepted++ })
	return out, nil
}

func rebalanceType(t ActionType) (multiraft.RebalanceActionType, bool) {
	switch t {
	case ActionMove:
		return multiraft.RebalanceMoveReplica, true
	case ActionSplit:
		return multiraft.RebalanceSplitRange, true
	case ActionTransferLeader:
		return multiraft.RebalanceTransferLeader, true
	}
	return 0, false
}
func (s *Service) reject(a Approval, cause error) (ValidationOutcome, error) {
	out := ValidationOutcome{Index: a.ActionIndex, Status: StatusRejectedValidator, Reason: cause.Error()}
	s.recordOutcome(a.AdviceID, a.Operator, out)
	s.increment(func(c *Counters) { c.ValidatorRejected++ })
	return out, fmt.Errorf("%w: %w", ErrRejectedByValidator, cause)
}
func (s *Service) recordOutcome(id uint64, operator string, out ValidationOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[id]
	r.ValidatorOutcomes = append(r.ValidatorOutcomes, cloneValidationOutcomes([]ValidationOutcome{out})[0])
	r.ApprovedBy = operator
	r.Advice.Status = out.Status
	s.records[id] = r
}

// LinkAction records observational provenance after a separate certified host
// has submitted a previously validated candidate. It performs no mutation.
func (s *Service) LinkAction(adviceID, actionID uint64, migrationID multiraft.MigrationID, splitID multiraft.SplitID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[adviceID]
	if !ok || actionID == 0 || len(r.ValidatorOutcomes) == 0 || r.ValidatorOutcomes[len(r.ValidatorOutcomes)-1].Status != StatusApprovedReview {
		return ErrAuditLink
	}
	if r.ActionID != 0 && (r.ActionID != actionID || r.MigrationID != migrationID || r.SplitID != splitID) {
		return ErrAuditLink
	}
	r.ActionID, r.MigrationID, r.SplitID = actionID, migrationID, splitID
	s.records[adviceID] = r
	s.counters.LinkedActions++
	return nil
}

func IsRejected(err error) bool { return errors.Is(err, ErrRejectedByValidator) }
