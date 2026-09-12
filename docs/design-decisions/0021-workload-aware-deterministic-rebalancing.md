# ADR-0021: Workload-aware deterministic rebalancing

**Status:** Accepted

## Context

Phases 7 and 8 provide certified split, migration and leadership-transfer
execution. Phase 9 needs automatic selection without turning noisy metrics or
controller memory into ownership authority.

## Decision

Use an observe/plan/validate/execute/reconcile controller. Planning consumes a
canonical immutable snapshot, integer EWMA telemetry and a versioned explicit
policy. Hard constraints precede a transparent weighted load/cost model;
projected-state improvement and stable identity tie-breaks make plans
deterministic. Replicated MetaRange control records preserve ActionIDs,
execution linkage, controller epoch and cooldown across restart. Execution calls
only the certified Phase 7/8 APIs.

Use NodeID as the Phase 9 failure domain. Select an interior current user-key
median for splits. Prefer leadership transfer for leader-only skew. Keep rate
history bounded and in memory; restart requires warm-up, while durable cooldown
does not disappear.

## Consequences

Decisions are replayable, dry-runnable and explainable, and controller failure
cannot alter data authority. Conservative warm-up/cooldown delays reaction.
Integer scoring is less expressive than a general optimizer but avoids floating
point and opaque heuristics. The policy finds deterministic improving local
moves, not globally optimal placement.

## Rejected alternatives

- Random greedy placement is simpler but cannot reproduce decisions.
- Direct catalog edits bypass certified membership/split protocols.
- Local-only history loses cooldown on failover and permits duplicate work.
- CPU-based or ML scoring would rely on telemetry/behavior the runtime cannot
  currently measure or certify.
- Joint split-and-move operations enlarge the failure state space without need.
