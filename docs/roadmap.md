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
| 1 | Local LSM storage engine | 🔨 in progress (1A/1B) |
| 2 | Single Raft group | ⬜ |
| 3 | Durable replicated range (Raft + storage) | ⬜ |
| 4 | Multi-Raft and range routing | ⬜ |
| 5 | MVCC | ⬜ |
| 6 | Distributed transactions | ⬜ |
| 7 | Online range splitting | ⬜ |
| 8 | Online replica migration | ⬜ |
| 9 | Workload-aware rebalancer | ⬜ |
| 10 | Chaos and correctness campaigns | ⬜ |
| 11 | Performance engineering | ⬜ |
| 12 | Optional AI operator | ⬜ |

**What exists right now:** Phase 0, the pre-Phase-1 internal-key contract and
Phase 1A/1B storage primitives/WAL. There is no complete key-value engine, Raft,
server or client. Anything else described in
[architecture.md](architecture.md) is a design, clearly marked as such.

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

## Phase 1 — Local storage engine ⬜

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

**Delivered so far — Phase 1A/1B/1C/1D:** authoritative internal-key and write-batch
codecs; versioned 32 KiB WAL block framing with independent header/content
CRC32C; bounded streaming reader; `SyncBatch` and `SyncNone`; concurrent append
serialization; clean restart; explicit truncated-tail repair; every-offset
truncation, systematic corruption, random-byte and failure-injection tests; WAL
microbenchmark baseline; concurrent skip-list MemTable with exact lookup,
lower-bound and version-candidate seek, stable iteration, half-open user-key
ranges, exact-key replacement, freeze and deterministic approximate memory
accounting; versioned SSTable writer with prefix-compressed restart blocks,
full-last-key index, metadata, checksummed typed blocks, fixed EOF footer,
bounded encoding and atomic durable publication. Evidence:
[`phase-1ab.md`](evidence/phase-1ab.md),
[`phase-1c.md`](evidence/phase-1c.md), and
[`phase-1d.md`](evidence/phase-1d.md).

**Remaining before Phase 1 is complete:** SSTable reader/seek/iteration, Bloom filter,
manifest/version set, active-to-immutable rotation and flush, compaction,
engine-level recovery, Get/Put/Delete/Scan and the full Phase 1 benchmark.

---

## Phase 2 — Single Raft group ⬜

Raft against an in-memory state machine, with an injected transport, an
injected clock and injected persistence, so that entire cluster scenarios run
deterministically in one process.

**Build:** leader election with randomized timeouts · `RequestVote` ·
`AppendEntries` with conflict resolution · commit and apply indices · durable
term, vote and log · snapshot creation and installation · log compaction ·
leader transfer.

**Gate:** a three-node cluster elects a leader and replicates writes; and each
of these scenarios preserves RAFT-1 through RAFT-12 with no acknowledged
committed write lost —

leader killed · follower killed · leader restarted · symmetric partition ·
**asymmetric** partition · an isolated old leader rejoining · dropped, delayed,
duplicated and reordered messages · a follower far enough behind to need a
snapshot · full-cluster restart · repeated randomized fault sequences from
recorded seeds.

---

## Phase 3 — Durable replicated range ⬜

Connect Raft to the storage engine, so a client write is replicated to a
majority and then applied durably.

**Build:** a state machine that applies committed entries to the storage engine
· snapshots built from an engine snapshot · idempotent apply keyed on log index
· a minimal RPC server and client.

**Gate:** `PUT → Raft → majority → apply → storage` end to end; every node
crashes and recovers with identical applied state; a snapshot-installing
follower converges to the leader byte-for-byte; an entry applied twice after a
restart does not corrupt state.

**Also decides:** the read path — Raft read, ReadIndex, or leader lease — as an
ADR, with the clock-assumption tradeoff spelled out.

---

## Phase 4 — Multi-Raft and routing ⬜

**Build:** range descriptors and the meta-range · many Raft groups per node
sharing a batched tick loop · a router with an immutable cached range map ·
stale-route detection via generation, with bounded retry.

**Gate:** keys route to the correct range · several ranges serve concurrently
with independent leaders · one node hosts many ranges without a goroutine per
range · a client with a deliberately stale map converges without error ·
RANGE-1 through RANGE-6 hold under randomized descriptor churn.

---

## Phase 5 — MVCC ⬜

**Build:** timestamped version encoding in the storage engine · write intents ·
transaction records · snapshot reads · write/write conflict detection · version
garbage collection.

**Gate:** concurrent transaction tests for MVCC-1 through MVCC-7 · a documented
anomaly table stating exactly which anomalies are prevented and which are
permitted, each entry backed by a test that demonstrates the behaviour · GC
never removes a version a live reader could see · restart preserves visibility.

**Also decides:** timestamp allocation and isolation level, each recorded in
the next available ADR at that phase.

---

## Phase 6 — Distributed transactions ⬜

**Build:** a coordinator · two-phase commit over per-range replicated
transaction state · idempotent intent resolution · timeout-based abort ·
recovery for both coordinator and participant crashes.

**Gate:** a two-range transfer commits atomically · a participant crashed
between prepare and commit recovers to the correct outcome · a coordinator
crashed after the commit point still results in a commit · a coordinator
crashed before it results in an abort · duplicated prepare/commit/abort
messages change nothing · a bank-transfer workload maintains a constant total
balance under continuous fault injection (TXN-1 through TXN-6).

---

## Phase 7 — Range splitting ⬜

**Build:** split-key selection by sampling · the split as a single Raft entry ·
new-range state creation · descriptor and generation updates · routing
invalidation · crash recovery mid-split.

**Gate:** reads and writes continue across a split under load, with the
throughput dip measured and published · stale clients converge · a crash
injected at every step of the split recovers cleanly · no key is lost or
duplicated (SPLIT-1 through SPLIT-4).

---

## Phase 8 — Replica migration ⬜

**Build:** learner replicas · snapshot streaming with flow control · catch-up
from the log · voter promotion · leadership transfer · voter removal ·
restartable migration state machine.

**Gate:** a range migrates while serving load · a node killed mid-migration
leaves quorum intact and the migration completes or rolls back · a duplicated
control-plane command produces one move · MIGRATE-1 through MIGRATE-6 hold
throughout.

---

## Phase 9 — Workload-aware rebalancer ⬜

The signature feature.

**Build:** per-range telemetry collection and aggregation · hotspot, node
overload, storage imbalance and leader concentration detectors · the proposal
generator · the safety validator · the admission controller with cooldown and
concurrency limits.

**Gate:** an artificially created hot range is detected · the proposed action
is the expected one, asserted by a unit test with a synthetic telemetry
snapshot and no cluster · execution completes under load · safety constraints
provably gate every action · a stable workload reaches a fixed point without
oscillating (BALANCE-1 through BALANCE-5) · **the before/after experiment
below**.

### The signature experiment

Five nodes, 32 ranges, 70% of traffic on two ranges. Measure per-node CPU, P50
and P99 latency, throughput and range distribution before rebalancing; enable
the controller; measure the same afterwards. Publish the method, the raw data,
the seed and the numbers — including any that are unflattering. Fabricated or
cherry-picked results would defeat the purpose of building this.

---

## Phase 10 — Chaos and correctness ⬜

**Build:** the fault-injection framework (`KillNode`, `RestartNode`,
`PauseNode`, `DropMessage`, `DelayMessage`, `DuplicateMessage`,
`PartitionNodes`, `HealPartition`, `ThrottleDisk`, `ThrottleNetwork`,
`CorruptWALRecord`) · seeded randomized campaigns · a history recorder · a
consistency checker for whatever RivetDB actually claims.

**Gate:** long campaigns run repeatedly with no invariant violation · every
failure's seed is committed and replays deterministically · the checker's scope
is documented precisely, and any property it does not check is listed as
unchecked.

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
(BALANCE-2) — it recommends, it never executes, and no database correctness
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
