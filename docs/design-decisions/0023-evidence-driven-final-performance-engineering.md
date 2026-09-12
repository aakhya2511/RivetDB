# ADR-0023: Evidence-driven final performance engineering and benchmark contract

Status: Accepted
Date: 2026-09-12
Phase: 11

## Context

RivetDB's complete local-to-control-plane stack now has correctness evidence,
but existing benchmarks were created phase by phase and cannot support one
unqualified performance headline. Several promising changes touch durability,
transaction authority, corruption detection or buffer ownership. The available
internal APFS volume is also nearly full, making disk timing nonrepresentative.

## Decision

Freeze a benchmark taxonomy with explicit timed boundaries, multiple samples,
direct bounded percentile sampling, allocations and environment metadata.
Classify results as CPU microbenchmark, local durable, in-process replicated,
control-plane, constrained-environment disk, or representative. Profile CPU,
allocation and blocking before selecting work. Retain at most five measured,
low- or medium-risk optimisations, each with A/B and focused correctness
evidence, then certify the exact final tree through compositional chaos and all
inherited gates.

The logical snapshot, eager validation, 2PC authority/idempotency, Raft order
and durability acknowledgement boundaries remain unchanged. Near-full storage
qualifies disk measurements instead of blocking unrelated profiling, and no
arbitrary free-space threshold is a correctness prerequisite.

## Consequences

Results are reproducible and difficult to overstate, while some attractive
changes may deliberately be rejected. In-process distributed timings include
real state machines and local persistence but no network stack. The current
host can publish CPU/allocation evidence and constrained disk baselines, not
representative local-disk performance. Full certification is expensive but is
the final authority for retained code.

The retained set is deliberately small: an empty-Version point-read fast path,
iterator-snapshot scan consumption and allocation-free projected planner
scoring. The Raft decode-arena experiment was reverted after a time regression;
parallel 2PC, group commit, larger snapshot chunks, block caching and lazy table
validation remain deferred or rejected. Exact A/B evidence lives in
`docs/evidence/phase-11.md` rather than being repeated normatively here.

## Rejected alternatives

- One aggregate benchmark score hides incompatible durability and transport
  boundaries.
- Optimising plausible hotspots before profiling creates unreviewable churn.
- Treating near-full APFS timings as representative would be misleading.
- Blocking all work on a free-space threshold discards valid CPU, allocation
  and correctness evidence.
- Lazy validation, raw-file snapshot transfer, one-phase commit or broad lock
  rewrites have cleaner-looking benchmark potential but excessive proof cost
  for Phase 11 without compelling profiles.
- Brittle latency thresholds in CI turn host noise into false correctness
  failures; reproducible commands and broad CPU guardrails are safer.

## Revisit if

A documented unconstrained local SSD/host, a real RPC transport, or profiles on
representative sustained workloads materially change the observed bottlenecks.
