# Phase 12 advisory AI operator

## 1. Design gate

1. The advisor may inspect a bounded canonical projection of Phase 9 node,
   range, replica, operation, cooldown and recent-action telemetry, plus
   environment-qualified Phase 11 performance summaries.
2. Raw user values, raw keys, transaction payloads, Raft entries, credentials,
   filesystem paths/secrets and unbounded logs never enter advisor input.
3. It may recommend NOOP, INVESTIGATE, WAIT, MOVE, SPLIT or TRANSFER_LEADER.
4. Direct membership, metadata, Raft, transaction, MVCC, durability, policy and
   recovery actions are forbidden and absent from the schema.
5. Advice is strict bounded JSON containing grounded findings and RangeID-only
   recommendations bound to snapshot, catalog and policy versions.
6. An executable recommendation is converted by the host through the existing
   Phase 9 planner and exact `ValidateRebalancePlan` against a fresh snapshot.
7. AI absence cannot change correctness or controller behavior because neither
   planner nor controller imports or calls the advisor.
8. Model failure cannot stop the data plane; it returns an advisor error only.
9. Unknown and retired identities fail closed during fresh resolution.
10. Generation/version changes return `ErrStaleAdvice`.
11. Telemetry strings are length-bounded JSON data, never interpolated into the
    fixed policy text; raw keys and arbitrary log text are omitted.
12. Advice records carry AdviceID, SHA-256 input digest, provider/model,
    prompt/schema versions, validation outcomes and optional ActionID linkage.
13. Planner/advisor disagreement is recorded; AI is never preferred
    automatically.
14. Approval records an operator label, time and fresh validation result. It
    returns a candidate for review and does not execute it.
15. Disabled mode constructs no snapshot and makes no model call; the database
    and deterministic controller are unchanged.

`PHASE 12 AI OPERATOR DESIGN GATE: PASS`

## 2. Authority and lifecycle

```text
canonical bounded snapshot -> model -> strict advice parser
                                      -> host approval
                                      -> Phase 9 PlanRebalance
                                      -> Phase 9 ValidateRebalancePlan(fresh)
                                      -> validated candidate for human host
```

There is intentionally no execution arrow in this package. A separate host may
submit the returned existing action through the certified controller APIs and
then link its authoritative ActionID to the observational AdviceID. Advice
history can be discarded without changing placement, cooldowns or recovery.

Requests are operator-triggered. Context cancellation and a configured timeout
bound model calls. Output and arrays are checked before allocation can become
unbounded. No continuous model loop, retry loop, mutation tool, CLI, external
dependency or live-provider adapter is required for certification.

## 3. Canonical input and privacy

Input contains schema/snapshot versions, catalog generation, policy version,
controller epoch; sorted node/range/replica numeric summaries; operation flags;
bounded cooldowns and recent action summaries; a derived health summary; and
bounded benchmark records whose classification is preserved. Range bounds and
sample keys are omitted. Errors are represented by fixed status categories,
not raw text.

An external provider, if later implemented behind `AdvisorModel`, sees exactly
the canonical JSON request and fixed policy contract. Credentials remain the
adapter/host's responsibility and are never stored in snapshots or records.
With no adapter configured, nothing leaves the process.

## 4. Structured output and failure behavior

Findings and recommendations contain cautious summaries and explicit grounded
evidence field/value/unit tuples. Recommendations contain an allowed type, RangeID,
reason, benefit, cost, constraints and uncertainty; they contain no split key,
target NodeID or ReplicaID. Unknown JSON fields, enums, IDs, duplicates,
same-range conflicting actions, missing evidence and oversized content reject
the entire response. Provider error, timeout, cancellation or malformed output
has no controller/data-plane side effect.

Confidence is self-reported HIGH/MEDIUM/LOW and is never authority. Numeric
claims must appear in supplied evidence. Phase 11 classifications remain
`CONSTRAINED-ENVIRONMENT`, `IN-PROCESS`, `EXACT-HOST CPU` or `NOT MEASURED`.

## 5. Certification boundary

All required tests use a deterministic mock. Parser fuzzing, a 10,000-cycle
random campaign, stale/unknown/conflict fixtures, provider failures,
disabled-mode equivalence and shadow chaos certify safety. Natural-language
quality and agreement with the deterministic planner are product observations,
not correctness properties. A live-model smoke test is optional and was not
used for certification.
