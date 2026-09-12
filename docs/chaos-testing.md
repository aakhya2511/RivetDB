# Compositional distributed chaos

## 1. Phase 10 design gate

This design composes the certified Phase 1--9 contracts; it introduces no new
database semantics. These answers are binding for the Phase 10 harness.

1. The authoritative checker is a logical `ClusterModel`: catalog/lineage,
   nodes, ranges/memberships, committed MVCC versions, transaction records and
   intents, split/migration/action records, controller epochs/cooldowns and
   conservation state. Physical SSTable layout is deliberately absent.
2. Every event checks ID uniqueness, range tiling, membership roles, terminal
   transaction monotonicity, operation exclusion, MVCC ordering, bounded state,
   acknowledged-mutation presence and the bank total.
3. Replica equality, latest/historical digests, transaction resolution,
   operation reconciliation, leader availability and controller NOOP are
   meaningful only after healing and enough healthy quorum steps.
4. Independent records represent overlapping work. A per-range operation slot
   serializes protocol-forbidden combinations; distinct ranges and compatible
   work overlap.
5. Directed partitions, delayed stale identities, joint-config crashes,
   full-cluster restart with several authorities in flight and control/data
   plane failures together are the least-tested fault classes.
6. Transactions may overlap migration, leadership and rebalancing; operations
   on distinct ranges may overlap; compaction may overlap logical transfer.
7. Split+migration, two migrations, two distinct splits, or two membership
   transitions on the same range are rejected or serialized.
8. Replay needs config version/profile, seed, event-prefix length, earliest
   failing event and a bounded ring with logical time and involved identities.
9. A seeded PRNG creates one single-threaded schedule; clocks are injected
   logical clocks and identities are traversed in stable sorted order.
10. Trace/history are rings, active objects and key/range counts are capped,
    and durable roots are owned temporary directories.
11. Tier A uses cheap logical state at high event count. Tier B invokes the
    filesystem-backed certified components at lower count. Neither substitutes
    for the other.
12. Lost/corrupt acknowledged state, conflicting authority, atomicity/SI
    failure, invalid quorum, changed history, identity resurrection or an
    existing invariant violation is `SAFETY`.
13. Quorum-loss unavailability is `AVAILABILITY`; missed post-heal convergence
    is `LIVENESS`; harness and host failures are `TEST-HARNESS`/`ENVIRONMENT`.
14. Recovery is reported in logical events and optionally observed Tier B wall
    time. Test bounds are never production SLAs.
15. Successful evidence retains configs, seeds/counts, aggregate counters,
    digests, recovery/resource observations, commands and environment. Detailed
    traces are emitted only on failure.

`PHASE 10 CHAOS DESIGN GATE: PASS`

## 2. Architecture and scheduling

`ChaosConfig` is versioned and records seed, event count, scale, operation and
fault weights, checkpoint/digest/full-restart frequencies and resource limits.
Smoke, normal, heavy and overnight are conveniences whose exact values are
printed. Fixed seeds precede a seed from `testutil.Seed`.
`RIVETDB_CHAOS_SEED` and `RIVETDB_CHAOS_EVENTS` replay an exact prefix.

Tier A keeps separate reference and observed logical views. Cheap checks run
each event; periodic checks compare canonical SHA-256 digests of sorted catalog,
membership, transaction and MVCC state. A 4,096-record ring is the only detailed
history. Tier B composes real `raft.FileStore`, replicated-range LSM, SSTables,
Manifests, metadata, split/migration/transaction/controller paths and snapshot
staging through the five-node Multi-Raft fixtures. Existing abrupt-process
matrices are run together by the Phase 10 crash tier.

## 3. Event and fault model

Events cover Put/Delete; transaction begin/read/scan/write/delete/commit/abort;
GetAt/ScanAt/snapshot; Raft tick and delivery/drop/delay/duplicate/reorder;
directed range and whole-node partition/heal; range/node crash/restart;
controller and MetaRange outage/recovery; flush/compaction/reclamation;
manual/automatic split/migration and transfer progress; leadership transfer;
and full-cluster restart. Stale parent, replica, coordinator and controller
identities are retained in bounded tombstone sets and redelivered.

Faults respect existing crash-stop/storage contracts. The harness does not
invent a filesystem that acknowledges data before required fsync. Existing
bounded short-write/fsync/corruption tests are composed into certification; the
host disk is never intentionally filled.

## 4. Checkers

Cheap checks cover all certified invariant namespaces expressible in logical
state and `CHAOS-1`--`CHAOS-16`: catalog ownership, monotonic term/config,
learner exclusion, joint dual majority, terminal outcomes, first-committer-wins,
operation identity/exclusion and acknowledged writes. After healing, expensive
checks require stable leaders where quorum exists, caught-up reference/replica
digests, exact latest and sampled history, transaction resolution, operation
reconciliation and repeated controller NOOP. Inspection uses non-accounting
status paths and has a regression test that telemetry is unchanged.

## 5. Overlap matrix

| Operations | Same range | Different ranges | Expected result |
|---|---:|---:|---|
| transaction + migration | yes | yes | safe; intent/history transferred |
| transaction + leader change | yes | yes | safe or authoritative retry |
| transaction + split | yes | yes | prepare fence/drain; safe retry |
| split + split | no | yes | same parent rejected |
| migration + migration | no | yes | same range rejected |
| split + migration | no | yes | same range rejected |
| membership + membership | no | yes | second transition rejected |
| rebalancer + transaction | yes | yes | certified APIs preserve semantics |
| compaction + image transfer | yes | yes | logical image is stable |

Targeted pairs cover transaction+migration, transaction+split,
migration+controller crash, split+MetaRange outage, joint config+node crash,
leader transfer+write, automatic split+bank transfer and automatic
migration+historical snapshot. Targeted triples cover transaction+migration+
leader change, split+controller crash+MetaRange outage, joint config+source
crash+directed partition and automatic rebalance+node restart+historical read.

## 6. Resources, classification and claim boundary

Model limits cap keys, transactions, ranges, trace and retained terminal state.
Durable tests compare goroutines and, where available, descriptors before/after
settlement. They audit only their RivetDB-owned root. Terminal authority records
are distinguished from active-operation leaks.

Failures are `SAFETY`, `LIVENESS`, `AVAILABILITY`, `TEST-HARNESS` or
`ENVIRONMENT`. Liveness assumes healed links, enough live voters, writable
storage and continued steps. ENOSPC qualifies evidence rather than becoming a
database failure unless RivetDB returns false success.

This provides deterministic reference-model and durable crash/recovery
evidence, not formal verification or Jepsen certification. It checks Snapshot
Isolation, not serializability or linearizable reads. Heavy/overnight runs stay
out of ordinary `make test`; `make certify-chaos` includes a bounded heavy run
and all inherited Phase 1--9 gates.
