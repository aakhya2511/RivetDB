# Deterministic workload-aware rebalancing

## 1. Design gate

`PHASE 9 REBALANCING DESIGN GATE: PASS`

Telemetry is advisory input; MetaRange catalog/split/migration records and user
range Raft remain authority. Instantaneous counters become injected-clock rate
samples and integer EWMA values; sizes and health are current observations.
Range/node load are explicit weighted integer scores. Hard constraints are
filtered before benefit/cost ranking. A hot/large range splits only with a
deterministic interior current user-key median. Pure leader skew selects a
cheaper leadership transfer. Otherwise projected placement evaluates moves.

Hysteresis, minimum samples, replicated cooldown/history and cluster/per-node
limits suppress oscillation. Active split/migration records consume capacity.
Convergence means no candidate exceeds admission thresholds or improves the
sum-squared node-load deviation. NOOP records its strongest explanation. Every
tie breaks by action kind, RangeID, source NodeID, target NodeID and ReplicaID.
Thus the same canonical snapshot and policy produce the same ordered plan.

## 2. Architecture

```text
Collector -> immutable ClusterSnapshot
Planner   -> deterministic dry-run Plan
Validator -> fresh catalog/generation/role/health/capacity checks
Executor  -> MoveReplica | SplitRange | TransferRangeLeadership
Reconcile -> authoritative operation IDs, completion and replicated cooldown
```

The executor contains no alternate migration, split or membership algorithm.
Plan validation rejects stale catalog/range generations and rechecks target
health, availability, duplicate placement, capacity, role, lag and current
cluster/per-node operation limits immediately before execution. A stopped
controller submits nothing and never aborts data-plane operations. MetaRange
unavailability pauses admission; user ranges continue independently.

## 3. Scoring and policy

For each node, components are normalized in parts-per-million against the
healthy-cluster mean:

```text
NodeLoad = bytesWeight*logicalBytesNorm
         + writeWeight*writeRateNorm
         + readWeight*readRateNorm
         + leaderWeight*leaderCountNorm
         + backlogWeight*applyBacklogNorm
         + replicaWeight*replicaCountNorm
```

Range load uses logical bytes, write/read rates and backlog with separately
configured weights. Migration cost is `max(1, estimatedHistoricalBytes)`.
Candidate rank is expected reduction in global sum-squared load deviation
divided by cost, using overflow-safe integer cross multiplication. Emergency
capacity never bypasses a safety rule.

The versioned policy configures enable/move/split/leader switches, sample
interval, EWMA alpha, minimum samples, start/recovery imbalance thresholds,
capacity limits, hot/large/minimum split thresholds, maximum ranges, minimum
benefit, cooldowns, and cluster/per-node concurrency. Invalid policy is rejected.

## 4. Split and leader choice

Split uses the median distinct current user key by key count. It never uses an
internal MVCC key and rejects bounds.
A single-key range is `HOT_UNSPLITTABLE_KEYSPACE`; no repeated attempt occurs.
Children receive fresh warm-up and inherited split cooldown. Because range merge
does not exist, defaults require sustained evidence and meaningful minimum size.

When physical/replica distribution is balanced but leader pressure exceeds the
threshold, a caught-up healthy existing voter is selected for transfer. This is
ranked before physical movement because no state copy is required.

## 5. Durable control state and recovery

MetaRange replicates policy version, controller epoch, monotonic ActionID,
action state, execution linkage, explanation, expected score and cooldown
deadlines. An action is recorded PLANNED before calling a certified primitive,
then EXECUTING and SUCCEEDED or FAILED. Restart increments
the controller epoch, reconciles referenced MigrationID/SplitID, and cannot
duplicate an already authoritative action. Existing split/migration records
exclude conflicting automatic plans on the same range. If execution is
interrupted after an authoritative operation record exists, the action remains
EXECUTING with that MigrationID or SplitID and recovery resumes the same
operation rather than marking it failed or submitting a duplicate.

## 6. Scope and complexity

Failure domain is NodeID. MetaRange membership, topology/locality, range merge,
GC, stronger isolation, ReadIndex, SQL, ML and AI execution are out of scope.
Candidate enumeration is `O(R*N)` plus deterministic `O(C log C)` ranking.
Snapshot memory is `O(N + R + replicas)` and history is policy-bounded.

## 7. Phase 11 performance audit

Projected move and leader-transfer scoring now computes the same sum-squared
potential without materializing a full projected node slice and lookup map for
every candidate. A reference implementation test requires exact score equality
for both action kinds. At 1,000 ranges this reduced the matched benchmark median
15.7%, bytes 29.8% and allocations 45.3%; collection and fresh validation
remain separately measured costs. Planning policy, tie-breaks, admission and
execution semantics are unchanged.

## 8. Phase 12 advisor boundary

The deterministic planner neither calls nor depends on AI. RangeID-only
advisor intents are matched to an independently produced plan; exact source,
target and split key remain planner outputs. The unchanged fresh-state
validator then rechecks generation, health, placement, lag, capacity,
cooldowns and split/migration concurrency before a candidate is approved for
human review. The advisor cannot begin or execute an action.
