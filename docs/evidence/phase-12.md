# Phase 12 advisory AI certification evidence

## Boundary and implementation

Phase 12 adds only `internal/advisor`. It imports Phase 9 telemetry/planning
types; `internal/multiraft` does not import it. The deterministic controller,
planner, validator and every data-plane package are unchanged. The advisor
exposes read-only snapshot queries, model analysis, human approval validation
and observational audit linkage. It exposes no execution or mutation method.

Canonical input deep-copies and sorts nodes, ranges, replicas, cooldowns,
actions and benchmark summaries. It omits range bounds/sample keys, user
values, transaction payloads, Raft log contents, credentials, filesystem data
and raw errors. SHA-256 over canonical JSON binds every advice record.

Structured output permits NOOP, INVESTIGATE, WAIT, MOVE, SPLIT and
TRANSFER_LEADER. Executable intents carry only RangeID: target nodes, replicas
and split keys remain deterministic planner decisions. Every numeric finding
must exactly match a supplied evidence field.

## Rejection and failure evidence

- Range999/Node-like invented placement detail cannot validate: unknown active
  RangeID returns `ErrUnknownIdentity`, while target NodeID is absent from the
  schema and an unknown field rejects the entire JSON.
- Catalog generation change between advice and approval returns
  `ErrStaleAdvice` before planning.
- Cooldown, active migration, unhealthy/no target and conflicting same-range
  recommendations fail closed.
- Invalid JSON, unknown actions, negative IDs, missing/ungrounded evidence,
  duplicates, unknown fields and oversized output return advisor errors.
- Mock outage, timeout, cancellation and panic affect only the Analyze caller.
  Bounded concurrent admission returns `ErrBusy`; configured minimum interval
  returns `ErrRateLimited`.

The successful audit fixture records a nonzero AdviceID, snapshot digest,
prompt/schema versions and human label; fresh Phase 9 validation returns a
leadership-transfer candidate. A separate simulated host linkage records
`AdviceID -> ActionID 44`. No MigrationID or SplitID applies to that fixture.

## Deterministic campaigns

The 10,000-cycle campaign recorded:

| Counter | Value |
|---|---:|
| generated/valid | 3,750 |
| invalid | 6,250 |
| stale | 1,250 |
| unknown identities | 1,250 |
| conflicting responses | 1,250 |
| validator accepted for review | 1,250 |
| validator rejected | 1,250 |
| safety violations | 0 |

The evaluation fixtures cover leader skew, hot range, storage overload,
cooldown, no eligible target, failed target, active migration and balanced
cluster. Shadow comparison records one planner agreement and one different
recommendation; agreement is not a correctness score.

Three identical 10,000-event Phase 10 logical schedules—advisor absent,
disabled, and enabled shadow analysis—produced identical latest/historical
digests and controller/event counters. The model is deterministic and offline;
no provider credentials, network or paid call is used.

The bounded Go fuzz run executed 1,066,163 parser cases in 11 seconds with no
panic or unsafe acceptance. The exact final `make advisor-fuzz` result is the
certification authority rather than this development-run count.

## Advisor processing performance

Five exact-host Apple M4 samples, separate from provider/database latency:

| Path | Median | Range | Allocation |
|---|---:|---:|---:|
| disabled Analyze | 20.44 ns | 20.21–21.48 ns | 0 B / 0 allocs |
| canonical small snapshot | 3.135 us | 3.025–3.307 us | 5,085 B / 22 allocs |
| strict parse and grounding | 6.106 us | 5.938–6.232 us | 7,744 B / 73 allocs |

No external-model latency was measured or mixed with Phase 11 database claims.

## Certification

`make certify-advisor` runs formatting/static checks, deterministic advisor
tests, advisor race, bounded fuzzing, shadow chaos, then the exact inherited
`make certify-chaos` chain. Final exact-tree results, commit and working-tree
state are reported after the freeze gate.
