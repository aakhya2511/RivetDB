# Phase 9 telemetry

## 1. Planning snapshot

`ClusterSnapshot` is an immutable, canonical view stamped with the injected
controller clock, catalog generation, policy version and controller epoch. It
contains sorted nodes, active ranges, replicas and authoritative split/
migration operations. Collection finishes before planning begins; the planner
never consults live nodes while ranking candidates.

Mechanically available node metrics are hosted replicas, leaders, logical
bytes, estimated historical bytes, read/write/request rates, voter apply backlog,
learner transfer load, in-progress operations and availability. Range metrics
include identity/generation/bounds, logical and physical bytes, current-user-key
samples, rates, leader, placement, commit/applied indexes, backlog, MVCC
watermark and operation state. Replica metrics and sampler state are keyed by
`(RangeID,ReplicaID,NodeID)` and record role, leadership, match/applied indexes,
lag, local bytes and health. CPU and memory are not reported because the current
runtime has no reliable per-range attribution.

Logical bytes mean latest visible user-key bytes and drive sharding quality.
Historical bytes sum canonical MVCC keys, values and internal-key trailers. It
is a deterministic logical estimate of movement cost, not a claim about exact
SSTable or filesystem allocation. Version count alone never selects a split
point.

## 2. Counters, rates and smoothing

Traffic instrumentation uses explicit atomic replica request counters; engine
maintenance scans and table operations are not traffic. Counters are saturating
and monotonically increasing.
The collector samples them through `clock.Clock`. A positive interval produces
`delta/seconds`; counter regression is treated as reset with zero delta. Clock
regression produces no sample and cannot expire cooldown. Rates use integer
EWMA:

```text
smoothed = (alphaPPM * sample + (1,000,000-alphaPPM) * previous) / 1,000,000
```

The first sample initializes a baseline only. Rate-driven actions require
`MinSamples`; hard byte/replica/health constraints do not. High-frequency rate
samples are bounded in memory and warm up after process restart. Replicated
action history and cooldown deadlines survive restart.

## 3. Bounds and identity

Snapshots are bounded by catalog node/range/replica limits and contain no
unbounded sample history. Learners and retired replicas are visible for
diagnostics but excluded from normal placement totals. A new ReplicaID starts a
new metric identity; parent samples are not copied to split children. A node
observed unavailable must accumulate `MinSamples` stable collector observations
after recovery before it is eligible as a placement target.

## 4. Phase 12 advisor projection

The optional advisor deep-copies this snapshot into a separately bounded,
canonically ordered projection. It retains numeric node/range/replica health,
load, lag and operation summaries but omits range bounds, sampled user keys,
values, command payloads and raw error text. Recent actions, cooldowns and
environment-qualified benchmark summaries are independently bounded. Advisor
inspection is non-accounting and does not update request counters or sampler
state.
