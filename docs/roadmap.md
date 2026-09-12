# RivetDB Roadmap

RivetDB is built in phases with explicit gates. A phase is complete when its
acceptance scenario runs and its evidence is recorded — not when the code
compiles. Work does not start on phase *N+1* until phase *N*'s gate passes,
because a bug in a lower layer discovered five phases later costs far more to
find than to prevent.

**Legend:** ✅ complete · 🔨 in progress · ⬜ not started

---

## Current status

| Phase | Scope | Status |
|---|---|---|
| 0 | Foundation: docs, module, build, CI, logging, test harness | ✅ |
| 1 | Local LSM storage engine | ✅ complete (1A–1K) |
| 2 | Single Raft group | ✅ complete |
| 3 | Durable replicated range (Raft + storage) | ✅ complete |
| 4 | Multi-Raft and static range routing | ✅ complete |
| 5 | Replicated MVCC and range-local snapshots | ✅ complete |
| 6 | Distributed transactions | ✅ |
| 7 | Online range splitting | ✅ complete |
| 8 | Online replica migration | ✅ |
| 9 | Workload-aware rebalancer | ✅ complete |
| 10 | Chaos and correctness campaigns | ✅ complete |
| 11 | Performance engineering | ⬜ |
| 12 | Optional AI operator | ⬜ |

**What exists right now:** Phase 0, the certified Phase 1 local latest-state
engine, the mechanically qualified Phase 2 single-group Raft core, Phase 3's
durable replicated range, Phase 4's static Multi-Raft node/catalog/router, and
Phase 5's replicated MVCC history/range-local snapshots, and Phase 6's
Snapshot Isolation transaction API, replicated intents/records and cross-range
2PC, and Phase 7's online transaction-safe range splitting through a replicated
metadata authority. There is no serializability claim.
There is no network database server, distributed read protocol or client.
Anything later in [architecture.md](architecture.md) is a design, clearly
marked as such.

---

## Phase 0 — Foundation ✅

Establish the ground the rest is built on, and prove the gate mechanism works
before there is anything complicated to gate.

**Delivered**

- [`internal/rlog`](../internal/rlog) — structured logging over `log/slog` with
  canonical attribute keys, context propagation, a runtime-adjustable level,
  and a `Recorder` handler so tests assert on structured events instead of
  substrings.
- [`internal/clock`](../internal/clock) — a `Clock` interface with a system
  implementation and a deterministic `Mock`. Every time-dependent subsystem
  (Raft elections, lease expiry, transaction timeouts, rebalancer cooldowns)
  takes a `Clock`, which is what will make those subsystems testable without
  sleeping.
- [`internal/invariant`](../internal/invariant) — named, typed assertions with
  an `Expensive()` tier for O(n) structural checks.
- [`internal/testutil`](../internal/testutil) — seeded randomness with an
  explicitly promoted failing-seed corpus, goroutine-leak detection, and
  bounded polling helpers.
- `make check` running format, vet, lint, tests and race tests; GitHub Actions
  running the same on Linux and macOS, plus a nightly job that explores fresh
  seeds.
- [architecture.md](architecture.md), [invariants.md](invariants.md), this
  roadmap, [correctness.md](correctness.md),
  [storage-engine.md](storage-engine.md), and ADRs
  [0001](design-decisions/0001-lsm-tree-over-b-tree.md) and
  [0002](design-decisions/0002-range-partitioning-over-hashing.md).

**Gate:** `make check` passes — `gofmt`, `go vet`, `golangci-lint`,
`go test ./...`, `go test -race -count=2 ./...`.

---

## Phase 1 — Local storage engine ✅

A durable, ordered, single-node key-value store with an on-disk format this
project owns. No network, no replication. Design:
[storage-engine.md](storage-engine.md).

**Build:** WAL with per-record checksums and replay · MemTable and sealed
immutable MemTable · SSTable writer and reader with a sparse index and Bloom
filter · manifest and version set · background flush · levelled compaction ·
tombstones · forward iterators · `Get`/`Put`/`Delete`/`Scan`.

**Gate**

1. Restarting after a clean shutdown returns every acknowledged write
   (STORAGE-1).
2. A simulated crash at every WAL write offset recovers to a prefix-consistent
   state; no acknowledged synchronous write is lost (STORAGE-1, STORAGE-3).
3. Corrupting a byte in any WAL record or SSTable block is detected, not served
   (STORAGE-2).
4. A randomized model test — thousands of operations against the engine and
   against an in-memory reference map — reports identical results, across
   flushes and compactions (STORAGE-5, STORAGE-7).
5. A key deleted and then compacted stays deleted (STORAGE-6).
6. Iterators are unaffected by concurrent flush and compaction (STORAGE-10).
7. A recorded baseline benchmark: sequential and random `Put`, point `Get`,
   `Scan`, with write amplification and space amplification measured.

**Delivered — Phase 1A/1B/1C/1D/1E/1F/1G/1H/1I/1J/1K:** authoritative internal-key and write-batch
codecs; versioned 32 KiB WAL block framing with independent header/content
CRC32C; bounded streaming reader; `SyncBatch` and `SyncNone`; concurrent append
serialization; clean restart; explicit truncated-tail repair; every-offset
truncation, systematic corruption, random-byte and failure-injection tests; WAL
microbenchmark baseline; concurrent skip-list MemTable with exact lookup,
lower-bound and version-candidate seek, stable iteration, half-open user-key
ranges, exact-key replacement, freeze and deterministic approximate memory
accounting; versioned SSTable writer with prefix-compressed restart blocks,
full-last-key index, metadata, checksummed typed blocks, fixed EOF footer,
bounded encoding and atomic durable publication; production hostile-input-safe
SSTable Open/Get/Seek/GetCandidate/full and range iteration with resident
validated indexes, restart search, concurrent reads and explicit Close;
serialized WAL-before-apply batches, atomic threshold rotation, a bounded FIFO
immutable worker, exact validated flush output, conservative failure retention
and retained-WAL replay; deterministic binary VersionEdits, append-only
Manifest recovery, crash-safe CURRENT, immutable Version installation,
collision-safe file/sequence recovery, live-table cross-checking and a durable
inclusive contiguous WAL replay frontier. Evidence:
[`phase-1ab.md`](evidence/phase-1ab.md),
[`phase-1c.md`](evidence/phase-1c.md),
[`phase-1d.md`](evidence/phase-1d.md),
[`phase-1e.md`](evidence/phase-1e.md),
[`phase-1f.md`](evidence/phase-1f.md), and
[`phase-1g.md`](evidence/phase-1g.md), and
[`phase-1h.md`](evidence/phase-1h.md). Phase 1H compaction preserves every
version and tombstone while atomically replacing overlap-closed L0/L1 inputs.

Phase 1I integrates those units into Open/Put/Delete/Get/Scan/Flush/Compact/Close,
sequence-bounded multi-source reads and restart recovery; evidence is in
[`phase-1i.md`](evidence/phase-1i.md).

Phase 1J makes assigned and published sequence authority explicit, tests atomic
multi-entry visibility at deterministic barriers, exercises real process exits,
enables obsolete-SSTable deletion only under an exclusive read/Version lifetime
proof, keeps physical WAL deletion candidate-only, and passes a 50,000-operation
reference campaign with 120 restarts; evidence is in
[`phase-1j.md`](evidence/phase-1j.md).

Phase 1K adds profile-justified frozen iteration, a bounded leased table-reader
cache, user-key Bloom filters in the reserved v1 block, lower-copy Scan result
assembly, reproducible benchmark placement and explicit certification tiers.
Its constrained-environment measurements, full correctness record and Raft
readiness audit are in [`phase-1k.md`](evidence/phase-1k.md).

---

## Phase 2 — Single Raft group ✅

Raft against an in-memory state machine, with an injected transport, an
injected clock and injected persistence, so that entire cluster scenarios run
deterministically in one process.

**Build:** leader election with randomized timeouts · `RequestVote` ·
`AppendEntries` with conflict resolution · commit and apply indices · durable
term, vote and log · snapshot creation and installation · log compaction.
Leader transfer is deliberately deferred behind the safety gate.

**Gate:** a three-node cluster elects a leader and replicates writes; and each
of these scenarios preserves RAFT-1 through RAFT-14 with no acknowledged
committed write lost —

leader killed · follower killed · leader restarted · symmetric partition ·
**asymmetric** partition · an isolated old leader rejoining · dropped, delayed,
duplicated and reordered messages · a follower far enough behind to need a
snapshot · full-cluster restart · repeated randomized fault sequences from
recorded seeds.

**Delivered:** deterministic synchronous core, independent memory/file Raft
stores, RequestVote/AppendEntries/InstallSnapshot, atomic snapshot compaction,
continuous executable safety checkers, fixed/fresh 3- and 5-node campaigns and
the `make certify-raft` gate. See [`evidence/phase-2.md`](evidence/phase-2.md).

---

## Phase 3 — Durable replicated range ✅

Connect Raft to the storage engine so one range's client mutation is replicated
to a majority and then applied locally on every caught-up replica.

**Delivered:** canonical bounded PUT/DELETE commands; a static range-scoped
deterministic runtime; Raft-index-ordered WAL-free LSM application; persisted
storage-mode identity; explicit MemTable applied-index coverage; atomic
Manifest table/frontier installation; proposal waiters; local inspection and
logical digests; durable 3-/5-node, restart, crash and randomized campaigns.

**Gate:** `PUT/DELETE → Raft → majority → leader-local apply → success`; caught-up
replicas agree logically despite different physical LSM layouts; unflushed
state reconstructs from retained committed Raft history; explicit durable
frontiers prevent both skipped and duplicate replay; uncommitted durable log
entries never apply. See [replicated-range.md](replicated-range.md) and
[evidence/phase-3.md](evidence/phase-3.md).

**Deferred deliberately:** LSM-integrated Raft snapshots, log compaction for an
integrated range, real transport/RPC, and a distributed read protocol. Local
inspection is stale-capable. Phase 3 does not choose leader leases or claim
linearizable reads; the read-path decision remains due before Phase 4 exposes a
client read surface.

---

## Phase 4 — Multi-Raft and static routing ✅

**Delivered:** canonical static range descriptors and a checksummed persisted
full-keyspace catalog · many independent durable Raft/LSM ranges per physical
node · one bounded range-aware transport · deterministic round-robin and fixed
worker-pool schedulers · immutable binary-search routing · range-scoped leader
hints · generation validation and bounded pre-admission retry.

**Gate:** exact binary boundaries route to one owner · three RF=3 ranges serve
with independent leaders/quorums and divergent physical layouts · failures and
restarts remain range scoped · 100 groups share bounded runtime resources ·
catalog/crash/randomized/real-filesystem campaigns pass with zero invariant or
digest mismatch. See [multiraft.md](multiraft.md),
[range-routing.md](range-routing.md), and [evidence/phase-4.md](evidence/phase-4.md).

**Deferred deliberately:** metadata consensus and dynamic descriptor churn,
online split, migration, dynamic membership, distributed reads and RPC.

---

## Phase 5 — Replicated MVCC and range-local snapshots ✅

**Delivered:** canonical replicated HLC timestamps · a distinct persistent MVCC
storage mode · timestamped PUT/DELETE commands · historical `GetAt`/`ScanAt` ·
range-local read-only snapshots · applied/durable timestamp watermarks ·
leader-change, clock-skew, crash, compaction and reclamation preservation.

**Gate:** `MVCC-1` through `MVCC-14` · fixed/fresh 10k-event reference campaigns
and opt-in 100k-event Multi-Raft campaign · exact tombstone/version visibility ·
lagging-replica refusal · snapshots stable across newer writes, flush,
compaction, reclamation and restart · every lower phase remains certified.
See [mvcc.md](mvcc.md) and [evidence/phase-5.md](evidence/phase-5.md).

**Deferred deliberately:** intents, write transactions, conflict detection,
2PC, isolation claims, distributed snapshots, MVCC GC and linearizable reads.

---

## Phase 6 — Distributed transactions ✅

**Delivered:** Snapshot Isolation transactions · 128-bit TxnID · one RT and CT ·
buffered read-your-writes · lazy participant serving barriers · replicated
transaction records and intents · first-committer-wins · epoch-fenced 2PC ·
logical intent interpretation · idempotent recovery and resolution.

**Gate:** two-, three- and eleven-range atomic commits · write/write conflicts
and deliberate write skew · coordinator/participant leader change · stale
epoch fencing · mixed-state full restart · real subprocess crash at every 2PC
stage · fixed/fresh 10k+ and opt-in 100k-event SI reference campaigns · intent
flush/compaction/restart preservation · all lower gates remain certified
(`TXN-1` through `TXN-17`). See [transactions.md](transactions.md) and
[evidence/phase-6.md](evidence/phase-6.md).

---

## Phase 7 — Range splitting ✅

**Build:** replicated MetaRange/dynamic catalog · monotonic RangeID/SplitID
allocation · immutable lineage · transaction fence/drain · logical MVCC image
at S · independent shadow children · original-timestamp delta replay with
parent-index frontiers · final fence F · atomic metadata cutover · redirects
and retained parent status archive.

**Gate:** ordinary writes during copy/catch-up · transaction fence and drain ·
exact MVCC history partition through F · stale refresh · parent/meta/child
leader changes · mid/post split restart · abrupt image/replay/fence/cutover
crashes · deep lineage and fixed/fresh 10k-event campaigns · all lower gates
(`SPLIT-1` through `SPLIT-18`). See [range-splitting.md](range-splitting.md),
[metadata-range.md](metadata-range.md), and [evidence/phase-7.md](evidence/phase-7.md).

---

## Phase 8 — Replica migration ✅

**Built:** learner replicas · snapshot streaming with flow control · catch-up
from the log · voter promotion · leadership transfer · voter removal ·
restartable migration state machine.

**Gate:** live follower and leader movement · prepared intent/TxnRecord and
historical MVCC preservation · dual-majority joint consensus · repeated fresh
ReplicaIDs · bounded resumable transfer · abrupt crashes from partial snapshot
through deletion · full lower-phase regression (`MIGRATE-1` through
`MIGRATE-18`, `RAFT-CONFIG-1` through `RAFT-CONFIG-6`). See
[replica-migration.md](replica-migration.md),
[raft-membership.md](raft-membership.md), and [evidence/phase-8.md](evidence/phase-8.md).

---

## Phase 9 — Workload-aware rebalancer ✅

The signature feature.

**Built:** per-range telemetry collection and aggregation · hotspot, node
overload, storage imbalance and leader concentration detectors · the proposal
generator · the safety validator · the admission controller with cooldown and
concurrency limits · replicated controller epoch/action history · dry-run plans
· certified migration/split/leadership execution adapters.

**Gate:** an artificially created hot range is detected · the proposed action
is the expected one, asserted by a unit test with a synthetic telemetry
snapshot and no cluster · execution completes through the Phase 7/8 protocols ·
safety constraints gate every action · a stable workload reaches a fixed point
without oscillating (`REBALANCE-1` through `REBALANCE-18`).

### Deferred Phase 11 performance experiment

Five nodes, 32 ranges, 70% of traffic on two ranges. Phase 11 will measure per-node CPU, P50
and P99 latency, throughput and range distribution before rebalancing; enable
the controller; measure the same afterwards. Publish the method, the raw data,
the seed and the numbers — including any that are unflattering. Fabricated or
cherry-picked results would defeat the purpose of building this. Phase 9 does
not publish a performance improvement claim from this correctness gate.

---

## Phase 10 — Chaos and correctness ✅

**Built:** a versioned deterministic scheduler · bounded event trace · global
logical catalog/membership/MVCC/transaction/operation reference model · cheap
continuous and periodic digest checkers · directed network, node/range,
controller, MetaRange and full-restart fault events · explicit overlap matrix ·
real five-node FileStore/LSM composition · combined abrupt-process crash tier ·
resource and orphan audits.

**Gate:** fixed/fresh normal campaigns total at least 100,000 events · a
one-million-event heavy run · exact replay and earliest-prefix reporting ·
continuous `CHAOS-1` through `CHAOS-16` checks · durable transaction, split,
migration, joint consensus, controller, historical MVCC and full-restart
composition · every Phase 1--9 gate remains green. See
[chaos-testing.md](chaos-testing.md) and [evidence/phase-10.md](evidence/phase-10.md).

---

## Phase 11 — Performance engineering ⬜

**Build:** the benchmark harness (throughput and latency percentiles for GET,
PUT, mixed, scan, transactions, cross-shard transactions) · uniform, Zipfian,
read-heavy, write-heavy, hot-key and hot-range workloads · 1/3/5-node
configurations · recovery time, election duration, migration duration and split
impact measurements.

**Gate:** profile-driven optimisation only. Each documented optimisation states
the measured problem, the diagnosed cause, the change and the measured result.
No optimisation is claimed without a before-and-after number.

---

## Phase 12 — Optional AI operator ⬜

Only after the database gates pass. A component that consumes telemetry and
explains *why* a range is hot or a node is overloaded, and suggests actions.
Its suggestions enter the same validator as every other proposal
(REBALANCE-2) — it recommends, it never executes, and no database correctness
property depends on it.

---

## Working rules

Applied to every phase, in order:

1. Read the existing code before designing.
2. Write or revise the design note.
3. State the invariant the phase must preserve.
4. Implement the smallest correct unit.
5. Write the tests, including the failure cases.
6. Run them. Run the race detector and the linters.
7. Run the phase acceptance scenario.
8. Record the evidence.
9. Commit only once the gate passes.

Correctness before breadth. A half-working feature that demos well is worth
less than a complete one with its failure modes tested.
