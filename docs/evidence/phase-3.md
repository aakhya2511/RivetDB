# Phase 3 evidence: durable single-range replication

Date: 2026-09-11. Phase 2 parent:
`7a9a2ed79fd731ce5400c7e191c08f0c9ae8ae89`. Certification source is the
Phase 3 commit containing this record.

## Result and design gate

`PHASE 3 INTEGRATION DESIGN GATE: PASS`.

The four settled decisions are: Raft index is replicated storage order; a
deterministic one-command `ApplyCommitted` boundary bypasses standalone client
writes; the quorum Raft log is durability authority and replicated mode emits
no data WAL; and a distinct Manifest frontier advances only with explicit
contiguous flushed-generation coverage. The alternatives and crash proof are
in [ADR-0015](../design-decisions/0015-raft-to-lsm-replicated-state-machine.md)
and [replicated-range.md](../replicated-range.md).

## Implemented boundary

`internal/replicatedrange` owns one static `RangeID`, distinct node/replica
identity, one Raft node/store, one replicated-mode Engine, state-machine decode,
bounded proposal waiters, local status/inspection and a SHA-256 logical digest.
The production shape is `node-N/ranges/R/{raft,data}`. The synchronous core has
no timer/transport goroutine; callers inject scheduling and message delivery.

Command v1 is `RVCM`, version/type/reserved, little-endian u32 key/value
lengths, then owned key/value bytes. PUT and DELETE are bounded by both SSTable
and Raft command limits. Every truncation, bad magic/version/type/reserved byte,
oversize length, trailing byte and DELETE value is rejected. Leaders validate
before proposal and replicas decode again during apply.

The Engine persists standalone versus replicated mode in Manifest v1 optional
critical TLVs. Legacy/no-field is standalone and retains its exact initial edit.
Replicated mode rejects `Put`, `Delete`, `WriteBatch`, local sequence allocation
and a pre-existing data WAL. `ApplyCommitted` installs
`WriteBatch{FirstSequence: raftIndex}` directly. Volatile same-index identity is
term plus SHA-256 command bytes; exact duplicates return already-applied and
conflicts fail. A recovered index at/below the durable frontier is explicitly
already applied.

## Frontier and crash matrix

Each nonempty generation records explicit inclusive applied-index coverage.
No-op gaps are carried as coverage without physical entries. FIFO Manifest
installation requires `first == current frontier + 1` and atomically installs
the validated table plus `ReplicatedAppliedThrough = last`. Compaction omits and
asserts equality of this field. SSTable sequence extrema are never consulted.

| Seam | Mechanical outcome |
|---|---|
| durable append before quorum | no state-machine call; local LSM unchanged |
| quorum commit before leader apply | later committed replay materializes the command |
| MemTable apply/publication before flush | restart frontier is lower; volatile state is discarded and replayed |
| durable SSTable before Manifest | table is classified orphan, excluded from reads, and command replays |
| durable Manifest before volatile publication | reopen recovers table and frontier atomically; command is skipped |
| during/after compaction | logical digest and applied frontier remain unchanged |
| full process exit without `Close` | quorum Raft stores recover; a current-term no-op re-establishes commitment and replay |

The Manifest tests inject the post-fsync/pre-publication seam and prove the
durable table/frontier pair wins after reopen. Separate orphan tests prove the
pre-Manifest table has no authority. Engine tests prove unflushed state vanishes
and is reconstructed, no WAL file exists, coverage gaps are rejected, and
compaction cannot advance the frontier. One end-to-end hook-order test observes
`RAFT_COMMITTED`, `APPLY_STARTED`, `MEMTABLE_APPLIED`,
`VISIBILITY_PUBLISHED`, `SSTABLE_DURABLE`, then
`MANIFEST_APPLIED_FRONTIER_DURABLE`; the hooks are observation-only and are not
part of correctness.

## Cluster evidence

The targeted real-filesystem suite runs static 3- and 5-node groups with one
filesystem-backed `RAFTSTATE` and one independent LSM directory per replica.
Each applies 80 mixed PUT/DELETE commands, deliberately flushes replicas at
different points, validates every local LSM, and compares applied index plus
ordered logical digest. Both groups finished at applied index 81 (the election
no-op plus 80 commands), with zero digest mismatches. Representative physical
live-table counts were `[8,5,1]`. Separate cases prove:

- an old leader's durable minority-only entry never mutates its LSM;
- success follows leader-local apply, and immediate leader loss preserves the
  mutation on the surviving majority;
- follower and full-cluster restart converge;
- a completely unflushed 30-command workload reconstructs from Raft history;
- three different durable frontiers cause inverse replay counts and converge;
- malformed committed bytes stop the Raft node before applied-index advance;
- a follower whose committed storage apply fails becomes fatal while the
  remaining two replicas retain quorum and commit subsequent commands;
- pre-admission cancellation admits nothing, post-admission client loss does
  not suppress apply, stepdown returns leadership loss, and shutdown drains all
  bounded waiters;
- physical live-table counts differ while logical digests match.

The partial-flush full-restart case recorded pre-crash durable frontiers
`[26,21,0]` and post-restart materialized replay counts `[5,10,30]`, then zero
digest mismatches.

The opt-in subprocess matrix exits the whole child process with no range/Engine
close at each exact integration hook: `RAFT_COMMITTED`, `APPLY_STARTED`,
`MEMTABLE_APPLIED`, `VISIBILITY_PUBLISHED`, `SSTABLE_DURABLE`, and
`MANIFEST_APPLIED_FRONTIER_DURABLE`. Every case reopens all three independent
Raft/LSM roots, elects a leader, converges, and recovers the mutation.

## Randomized integration

The normal LSM-backed deterministic simulator runs 10,000 events per campaign
over fixed 3-node seed `301`, fixed 5-node seed `501`, and one fresh 3-node
seed. Actions include ticks, delivery/drop, directed link change/heal, PUT and
DELETE proposals, process-model crash/reopen, local flush and local compaction.
Every event runs the Raft safety checker; convergence runs every Engine
structural validator and compares full logical state. The final recorded run
used fixed seeds `301` and `501` plus fresh seed `-8818554942935924662`:
30,000 events, 422 accepted proposals, 639 crashes, 639 restarts, 899 flushes,
25 successful compactions, and zero digest mismatches.

The opt-in five-node `RIVETDB_RANGE_STRESS=1` tier runs 100,000 events. The
certification run used seed `5588339770393004871`: 684 accepted proposals,
2,370 crashes, 2,370 restarts, 3,065 flushes, 35 successful compactions and
zero digest mismatches. It took 4,209.30 seconds on the constrained APFS
volume. The range never invokes Raft snapshots or log compaction:
LSM-integrated snapshot export/replace-state restore is deliberately deferred,
so no history required for local recovery is discarded.

## Proposal and read semantics

Proposal success means quorum commit plus leader-local complete apply. It does
not wait for an LSM flush because durability is the committed Raft log. An error
after durable admission can be ambiguous; there is no client deduplication.
`LocalGet` and `LocalScan` are stale-capable replica inspection only. There is
no ReadIndex, lease, or linearizable distributed read claim.

## Engineering baseline and environment

These are `CONSTRAINED-ENVIRONMENT BASELINE` values, not representative
production or network measurements. The filesystem was local APFS
(`/dev/disk3s5`), 228 GiB capacity, 8.8 GiB available and 96% utilized. Test and
benchmark roots were Go temporary directories on that volume; no external SSD
was used. Hardware was a MacBook Air `Mac16,12`, Apple M4 (10 cores), 16 GiB;
OS macOS 15.7.4 (24G517), Darwin arm64; Go 1.25.14 primary and 1.27.1 forward
check. Power reported AC while the battery was 40% and discharging.

Three fixed-work samples measured:

| Local engineering operation | Median | Scope |
|---|---:|---|
| three-node durable propose → quorum commit → all local apply | 25.801 ms/op | 100 operations/sample; local FileStore fsyncs, no network |
| one follower catches up 100 commands | 15.842 ms | one operation/sample |
| full three-node reopen/election/replay of 100 unflushed commands | 98.476 ms | one operation/sample |

The propose sample reported about 65,723 B and 491 allocations/op. Filesystem
pressure and unstable power qualify all elapsed values; no final distributed
latency or throughput claim is made.

## Snapshot and Multi-Raft readiness

`LSM-INTEGRATED RAFT SNAPSHOT: DEFERRED`. Integrated range log compaction is not
exposed, the complete required history stays retained, and Phase 2 snapshot
support remains independently tested. Log growth/restart time are known limits.

RangeID, ReplicaID, Raft Node/Store, Engine, directories, frontier and waiters
are per instance. One process can construct many independent replicas today.
No new resource is accidentally global; the deterministic scheduler and
transport are external. Phase 4 must add descriptors/routing, map requests to a
RangeID, multiplex many groups over shared transport, and batch tick/work
scheduling to avoid per-group timer/goroutine explosion. It must not add
Multi-Raft behavior to this Phase 3 freeze.

## Scope and certification

REPLICA-1 through REPLICA-14 are verified by command/Engine/Manifest/range,
subprocess and randomized tests. The `REPLICA` namespace distinguishes this
Raft-to-state-machine boundary from Phase 4's reserved `RANGE` routing
invariants.

No Multi-Raft, multiple ranges, routing, dynamic membership, ReadIndex,
linearizable reads, MVCC, distributed transactions, range splitting, replica
migration, rebalancing or AI operator was implemented.

Certification commands are `range-test`, `range-race`, `range-stress`,
`range-crash` and `certify-range`; the latter also runs unchanged
`certify-raft` and `certify-local`. The final commit was created only after
formatting, Go 1.25/1.27 vet/tests, lint, normal/race/stress/crash tiers and all
three certifications passed with a clean diff.
