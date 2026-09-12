package multiraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
)

type RebalanceHookStage uint8

const (
	RebalancePlanBuilt RebalanceHookStage = iota
	RebalancePlanValidated
	RebalanceActionDurable
	RebalanceActionExecutingDurable
	RebalanceCertifiedOperationReturned
	RebalanceActionTerminalDurable
)

type RebalanceHook func(RebalanceHookStage, RebalanceActionRecord)

type RebalanceControllerOptions struct {
	Meta      *MetaRange
	Router    *Router
	Clock     clock.Clock
	Policy    RebalancePolicy
	Migration *MigrationManager
	Split     *SplitManager
	Hook      RebalanceHook
}

type RebalanceController struct {
	mu        sync.Mutex
	started   bool
	meta      *MetaRange
	router    *Router
	clock     clock.Clock
	policy    RebalancePolicy
	collector *RebalanceCollector
	migration *MigrationManager
	split     *SplitManager
	hook      RebalanceHook
}

func NewRebalanceController(options RebalanceControllerOptions) (*RebalanceController, error) {
	if options.Meta == nil || options.Router == nil || options.Clock == nil {
		return nil, ErrInvalidTelemetry
	}
	if err := options.Policy.Validate(); err != nil {
		return nil, err
	}
	collector, err := NewRebalanceCollector(options.Meta, options.Router, options.Clock, options.Policy.EWMAAlphaPPM, options.Policy.MinSamples)
	if err != nil {
		return nil, err
	}
	migration := options.Migration
	if migration == nil {
		migration, err = NewMigrationManager(MigrationManagerOptions{Meta: options.Meta, Router: options.Router})
		if err != nil {
			return nil, err
		}
	}
	split := options.Split
	if split == nil {
		split, err = NewSplitManager(SplitManagerOptions{Meta: options.Meta, Router: options.Router})
		if err != nil {
			return nil, err
		}
	}
	return &RebalanceController{meta: options.Meta, router: options.Router, clock: options.Clock, policy: options.Policy,
		collector: collector, migration: migration, split: split, hook: options.Hook}, nil
}

func (c *RebalanceController) Activate(ctx context.Context) (uint64, error) {
	if err := c.meta.SetRebalancePolicy(ctx, c.policy); err != nil {
		return 0, err
	}
	epoch, err := c.meta.TakeoverRebalanceController(ctx)
	if err == nil {
		c.mu.Lock()
		c.started = true
		c.mu.Unlock()
	}
	return epoch, err
}

// Stop prevents new plans without canceling certified split or migration
// state. A later Activate takes a new replicated controller epoch.
func (c *RebalanceController) Stop() { c.mu.Lock(); c.started = false; c.mu.Unlock() }

func (c *RebalanceController) running() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

func (c *RebalanceController) Observe(ctx context.Context) (RebalanceClusterSnapshot, error) {
	return c.collector.Collect(ctx)
}

func (c *RebalanceController) Plan(ctx context.Context) (RebalancePlan, error) {
	snapshot, err := c.collector.Collect(ctx)
	if err != nil {
		return RebalancePlan{}, err
	}
	return PlanRebalance(snapshot, c.policy, c.meta.Snapshot().Rebalance.Cooldowns)
}

func (c *RebalanceController) RunCycle(ctx context.Context) (RebalancePlan, error) {
	if !c.running() {
		return RebalancePlan{}, ErrRebalanceDisabled
	}
	if err := c.Reconcile(ctx); err != nil {
		return RebalancePlan{}, err
	}
	plan, err := c.Plan(ctx)
	if err != nil {
		return RebalancePlan{}, err
	}
	c.observe(RebalancePlanBuilt, RebalanceActionRecord{})
	if !c.policy.Enabled {
		return plan, nil
	}
	fresh, err := c.collector.Collect(ctx)
	if err != nil {
		return plan, err
	}
	if err := ValidateRebalancePlan(plan, fresh, c.policy); err != nil {
		return plan, err
	}
	c.observe(RebalancePlanValidated, RebalanceActionRecord{})
	for index := range plan.Actions {
		record, beginErr := c.meta.BeginRebalanceActionWithLimit(ctx, plan.Actions[index], c.clock.Now(), c.policy.HistoryLimit)
		if beginErr != nil {
			return plan, beginErr
		}
		plan.Actions[index].ActionID = record.Action.ActionID
		c.observe(RebalanceActionDurable, record)
		if _, advanceErr := c.meta.AdvanceRebalanceAction(ctx, record.Action.ActionID, RebalanceActionExecuting, 0, 0, "", c.clock.Now(), time.Time{}, time.Time{}, time.Time{}); advanceErr != nil {
			return plan, advanceErr
		}
		c.observe(RebalanceActionExecutingDurable, record)
		migrationID, splitID, executeErr := c.execute(ctx, record.Action)
		c.observe(RebalanceCertifiedOperationReturned, record)
		if executeErr != nil {
			discoveredMigration, discoveredSplit := c.discoverOperationIDs(record.Action)
			if migrationID == 0 {
				migrationID = discoveredMigration
			}
			if splitID == 0 {
				splitID = discoveredSplit
			}
		}
		if executeErr != nil && c.operationStillAuthoritative(record.Action.Type, migrationID, splitID) {
			if _, persistErr := c.meta.AdvanceRebalanceAction(ctx, record.Action.ActionID, RebalanceActionExecuting, migrationID, splitID, executeErr.Error(), c.clock.Now(), time.Time{}, time.Time{}, time.Time{}); persistErr != nil {
				return plan, persistErr
			}
			return plan, fmt.Errorf("execute rebalance action %d pending authoritative recovery: %w", record.Action.ActionID, executeErr)
		}
		terminal, lastError, cooldown := RebalanceActionSucceeded, "", c.policy.Cooldown
		if executeErr != nil {
			terminal, lastError, cooldown = RebalanceActionFailed, executeErr.Error(), c.policy.FailureCooldown
		}
		if record.Action.Type == RebalanceSplitRange && executeErr == nil {
			cooldown = c.policy.SplitCooldown
		}
		until := c.clock.Now().Add(cooldown)
		completed, persistErr := c.meta.AdvanceRebalanceAction(ctx, record.Action.ActionID, terminal, migrationID, splitID, lastError, c.clock.Now(), until, until, until)
		if persistErr != nil {
			return plan, persistErr
		}
		c.observe(RebalanceActionTerminalDurable, completed)
		if executeErr != nil {
			return plan, fmt.Errorf("execute rebalance action %d: %w", record.Action.ActionID, executeErr)
		}
	}
	return plan, nil
}

func (c *RebalanceController) discoverOperationIDs(action RebalanceAction) (MigrationID, SplitID) {
	metadata := c.meta.Snapshot()
	if action.Type == RebalanceMoveReplica {
		for index := len(metadata.Migrations) - 1; index >= 0; index-- {
			record := metadata.Migrations[index]
			if record.RangeID == action.RangeID && record.SourceReplicaID == action.SourceReplicaID && record.TargetNodeID == action.TargetNodeID {
				return record.MigrationID, 0
			}
		}
	}
	if action.Type == RebalanceSplitRange {
		for index := len(metadata.Splits) - 1; index >= 0; index-- {
			record := metadata.Splits[index]
			if record.Parent.RangeID == action.RangeID && bytes.Equal(record.SplitKey, action.SplitKey) {
				return 0, record.SplitID
			}
		}
	}
	return 0, 0
}

func (c *RebalanceController) operationStillAuthoritative(actionType RebalanceActionType, migrationID MigrationID, splitID SplitID) bool {
	metadata := c.meta.Snapshot()
	if actionType == RebalanceMoveReplica && migrationID != 0 {
		for _, record := range metadata.Migrations {
			if record.MigrationID == migrationID {
				return record.State != MigrationAborted && record.State != MigrationSourceRetired
			}
		}
	}
	if actionType == RebalanceSplitRange && splitID != 0 {
		for _, record := range metadata.Splits {
			if record.SplitID == splitID {
				return record.State != SplitAborted && record.State != SplitCommitted
			}
		}
	}
	return false
}

func (c *RebalanceController) Reconcile(ctx context.Context) error {
	history := c.meta.Snapshot().Rebalance.History
	if len(history) == 0 {
		return nil
	}
	record := history[len(history)-1]
	if record.State == RebalanceActionSucceeded || record.State == RebalanceActionFailed {
		return nil
	}
	now := c.clock.Now()
	if now.Before(record.UpdatedAt) {
		now = record.UpdatedAt
	}
	if record.State == RebalanceActionPlanned {
		_, err := c.meta.AdvanceRebalanceAction(ctx, record.Action.ActionID, RebalanceActionExecuting, 0, 0, "", now, time.Time{}, time.Time{}, time.Time{})
		if err != nil {
			return err
		}
	}
	migrationID, splitID, err := c.reconcileAction(ctx, record.Action)
	terminal, lastError, cooldown := RebalanceActionSucceeded, "", c.policy.Cooldown
	if err != nil {
		terminal, lastError, cooldown = RebalanceActionFailed, err.Error(), c.policy.FailureCooldown
	}
	until := now.Add(cooldown)
	_, persistErr := c.meta.AdvanceRebalanceAction(ctx, record.Action.ActionID, terminal, migrationID, splitID, lastError, now, until, until, until)
	if persistErr != nil {
		return persistErr
	}
	return err
}

func (c *RebalanceController) reconcileAction(ctx context.Context, action RebalanceAction) (MigrationID, SplitID, error) {
	metadata := c.meta.Snapshot()
	if action.Type == RebalanceMoveReplica {
		for index := len(metadata.Migrations) - 1; index >= 0; index-- {
			record := metadata.Migrations[index]
			if record.RangeID != action.RangeID || record.SourceReplicaID != action.SourceReplicaID || record.TargetNodeID != action.TargetNodeID {
				continue
			}
			switch record.State {
			case MigrationSourceRetired:
				return record.MigrationID, 0, nil
			case MigrationAborted:
				return record.MigrationID, 0, fmt.Errorf("%w: %s", ErrMigrationConflict, record.LastError)
			default:
				recovered, err := c.migration.RecoverMigration(ctx, record.MigrationID)
				return recovered.MigrationID, 0, err
			}
		}
	}
	if action.Type == RebalanceSplitRange {
		for index := len(metadata.Splits) - 1; index >= 0; index-- {
			record := metadata.Splits[index]
			if record.Parent.RangeID != action.RangeID || !bytes.Equal(record.SplitKey, action.SplitKey) {
				continue
			}
			switch record.State {
			case SplitCommitted:
				return 0, record.SplitID, nil
			case SplitAborted:
				return 0, record.SplitID, fmt.Errorf("%w: %s", ErrSplitConflict, record.LastError)
			default:
				recovered, err := c.split.RecoverSplit(ctx, record.SplitID)
				return 0, recovered.SplitID, err
			}
		}
	}
	if action.Type == RebalanceTransferLeader {
		if target := c.router.scheduler.Nodes()[action.TargetNodeID]; target != nil {
			if replica, err := target.Replica(action.RangeID); err == nil && replica.Status().Raft.Role == raft.Leader {
				return 0, 0, nil
			}
		}
	}
	return c.execute(ctx, action)
}

func (c *RebalanceController) execute(ctx context.Context, action RebalanceAction) (MigrationID, SplitID, error) {
	switch action.Type {
	case RebalanceMoveReplica:
		record, err := c.migration.MoveReplica(ctx, action.RangeID, action.SourceReplicaID, action.TargetNodeID)
		return record.MigrationID, 0, err
	case RebalanceSplitRange:
		record, err := c.split.SplitRange(ctx, action.RangeID, action.SplitKey)
		return 0, record.SplitID, err
	case RebalanceTransferLeader:
		return 0, 0, c.transferLeadership(ctx, action)
	default:
		return 0, 0, ErrNoEligibleAction
	}
}

func (c *RebalanceController) transferLeadership(ctx context.Context, action RebalanceAction) error {
	nodes := c.router.scheduler.Nodes()
	source := nodes[action.SourceNodeID]
	if source == nil {
		return ErrNodeStopped
	}
	out, err := source.TransferRangeLeadership(action.RangeID, action.TargetNodeID)
	if err != nil {
		return err
	}
	for _, envelope := range out {
		if err := c.router.transport.Send(envelope); err != nil {
			return err
		}
	}
	for range c.router.maxWork {
		target := nodes[action.TargetNodeID]
		if target != nil {
			if replica, lookupErr := target.Replica(action.RangeID); lookupErr == nil && replica.Status().Raft.Role == raft.Leader {
				return nil
			}
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for leadership transfer: %w", err)
		}
		if err := c.router.scheduler.Round(); err != nil && !errors.Is(err, ErrUnknownRange) && !errors.Is(err, ErrNodeStopped) {
			return err
		}
	}
	return ErrLeaderUnknown
}

func (c *RebalanceController) observe(stage RebalanceHookStage, record RebalanceActionRecord) {
	if c.hook != nil {
		c.hook(stage, record)
	}
}
