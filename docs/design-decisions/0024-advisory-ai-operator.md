# ADR-0024: Advisory AI operator outside deterministic authority

Status: Accepted
Date: 2026-09-12
Phase: 12

## Context

Operational telemetry can support useful diagnosis and explanations, but model
output is nondeterministic, fallible and may contain invented identities. The
Phase 9 controller and Phase 7/8 protocols already own safe action selection,
validation and execution.

## Decision

Add a provider-independent advisory package that consumes bounded canonical
read-only snapshots and parses only strict structured output. Executable
recommendations name an intent and RangeID only. The host derives exact
parameters through `PlanRebalance`, applies the unchanged
`ValidateRebalancePlan` against fresh state, and requires human approval. The
advisor exposes no mutation API. Advice/audit history is observational.

Certification uses a deterministic mock and offline fuzz/random/chaos tests.
Raw user data, raw keys, Raft commands, secrets and unbounded errors/logs are
excluded. Provider failures fail closed and cannot reach the control plane.

## Consequences

AI can explain and recommend without becoming availability, recovery or safety
authority. Target-node or split-key preferences are intentionally unavailable,
and a planner may decline an otherwise plausible recommendation. Live-provider
adapters and autonomous submission remain optional future host integrations.

## Rejected alternatives

- Direct mutation tools make model output part of the authority chain.
- A separate AI validator would duplicate and drift from Phase 9 constraints.
- Sending raw logs, keys or command payloads adds privacy and injection risk
  without improving placement decisions.
- Letting AI select targets or split keys unnecessarily trusts invented detail.
- Requiring a live provider would make correctness certification paid,
  nondeterministic and network-dependent.

## Revisit if

A future host provides authenticated operator identity and a separately
certified submission workflow, or telemetry proves RangeID-only intent cannot
support a required advisory use case.
