# RivetDB documentation

| Area | Design and evidence |
|---|---|
| System boundaries | [Architecture](architecture.md) and [diagrams](diagrams.md) |
| Local storage | [Storage engine](storage-engine.md) |
| Consensus | [Raft](raft.md), [membership](raft-membership.md), [replicated range](replicated-range.md) |
| Sharding | [Multi-Raft](multiraft.md), [routing](range-routing.md), [MetaRange](metadata-range.md) |
| Data model | [MVCC](mvcc.md) and [transactions](transactions.md) |
| Topology changes | [Range splitting](range-splitting.md), [replica migration](replica-migration.md) |
| Control plane | [Telemetry](telemetry.md), [rebalancing](rebalancing.md) |
| Verification | [Correctness](correctness.md), [chaos](chaos-testing.md), [invariants](invariants.md) |
| Measurements | [Performance](performance.md), [measured evidence](evidence/phase-11.md) |
| Optional advisor | [AI operator](ai-operator.md), [certification evidence](evidence/phase-12.md) |

The [ADR index](design-decisions/README.md) summarizes accepted decisions and rejected
alternatives. The [roadmap](roadmap.md) retains chronology; the evidence directory
contains exact environment, gate, failure, and measurement records.

## Tests worth reading

- internal/storage/wal/wal_test.go — exhaustive truncation and corruption recovery.
- internal/raft/membership_test.go — joint-consensus safety counterexamples.
- internal/multiraft/transaction_crash_test.go — transaction crash matrix.
- internal/multiraft/split_test.go — online split, transaction fences, stale routing.
- internal/multiraft/migration_crash_test.go — migration crash-stage recovery.
- internal/multiraft/migration_test.go — learner cutover and ReplicaID retirement.
- internal/multiraft/phase10_durable_test.go — real-filesystem composition.
- internal/chaos/model_test.go — one-million-event deterministic campaign.
- internal/advisor/advisor_test.go — stale and hallucinated advice rejection.

Executable gates, not summaries, are the correctness authority.
