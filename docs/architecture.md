# RivetDB Architecture

**Status:** design baseline for the implementation. Phase 0, Phase 1A through
1K, the Phase 2 single-group Raft core, the Phase 3 static durable replicated
range, the Phase 4 static Multi-Raft/routing composition, and Phase 5
replicated MVCC/range-local snapshots are implemented. See
[roadmap.md](roadmap.md) for what exists today
and [invariants.md](invariants.md) for the properties each subsystem must
uphold.

This document describes the intended structure of RivetDB and, more
importantly, *why* each boundary sits where it does. It is written to be
falsifiable: every claim about behaviour is either marked as implemented with a
pointer to the test that demonstrates it, or marked as planned.

---

## 1. Problem statement

RivetDB is an experimental distributed transactional key-value store. It exists
to explore one question:

> When a workload concentrates on a small part of the keyspace, can a database
> reshape its own data distribution — splitting ranges, moving replicas,
> redistributing leadership — while continuing to serve reads and writes
> correctly?

Every architectural decision is judged against that question. A design that
makes the steady state marginally faster but makes online reconfiguration
unsafe is the wrong design for this project.

### Non-goals

These are excluded deliberately, not for lack of time:

- **SQL.** A query planner adds a large amount of work that exercises none of
  the distributed-systems properties this project is about. The client API is
  key-value plus transactions.
- **Geo-distribution and clock synchronisation.** RivetDB does not assume
  bounded clock skew (no TrueTime-style commit waits). Timestamps come from a
  logical scheme described in §7. This restricts the isolation levels
  reachable, and that restriction is documented rather than papered over.
- **Multi-tenancy, authentication, encryption at rest.** Basic input hygiene is
  in scope (§11); a security model is not.
- **Production operation.** RivetDB is a research and demonstration system. It
  has no upgrade story, no backup tooling, and no operational track record.

---

## 2. System overview

RivetDB is a shared-nothing cluster of identical nodes. There is no special
coordinator process: the control plane runs on the same nodes as the data
plane, and its own state is replicated by the same mechanism as user data.

### 2.1 The read/write path

```text
                         ┌──────────────┐
                         │    Client    │
                         │  GET / PUT   │
                         │ DELETE/SCAN  │
                         │  BEGIN/COMMIT│
                         └──────┬───────┘
                                │ RPC
                                ▼
                    ┌───────────────────────┐
                    │   Router / Gateway    │   cached range map;
                    │  key → range → leader │   retries on stale route
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Range  [start, end)  │   one shard of the keyspace
                    │  descriptor, replicas │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │      Raft group       │   one per range; replicates
                    │ leader + followers    │   this range's log only
                    └───────────┬───────────┘
                                │ committed entries, applied in order
                                ▼
                    ┌───────────────────────┐
                    │      MVCC layer       │   versions, intents,
                    │  visibility, conflicts│   snapshot reads
                    └───────────┬───────────┘
                                │ encoded (key, timestamp) → value
                                ▼
                    ┌───────────────────────┐
                    │  LSM storage engine   │   WAL, MemTable, SSTables,
                    │  local, single node   │   compaction, Bloom filters
                    └───────────────────────┘
```

Each layer has a single responsibility and a narrow interface to the one below:

| Layer | Owns | Does not know about |
|---|---|---|
| Router | key → range → leader mapping, retry policy | Raft, MVCC, storage |
| Range | key bounds, replica set, descriptor version | how bytes are stored |
| Raft | log replication, leader election, snapshots | what the commands mean |
| MVCC | version visibility, write intents, conflicts | replication, ranges |
| Storage | durable ordered bytes on one disk | that a cluster exists |

The direction of ignorance matters. The storage engine is a standalone,
testable local database; it can be exercised for crash-correctness without any
network. The Raft implementation replicates opaque byte commands; it can be
tested against an in-memory state machine with no disk. Composing them is a
separate concern with its own tests. This is what makes each phase's gate
meaningful — a bug found in Phase 5 cannot be a Phase 1 bug in disguise.

Phase 3 composes these layers without reversing that dependency: the Raft core
still sees opaque commands and the storage engine still sees ordered mutations.
`internal/replicatedrange` owns decoding, proposal completion and the mapping
from Raft index to storage sequence. Its replicated engine mode treats the
quorum Raft log as durability authority and creates no standalone data WAL.
Each `RangeID` has independent `raft/` and `data/` directories, an Engine,
Raft Store, applied frontier and waiter set. No process-global range singleton
or timer was introduced. Phase 4 now places many instances behind
`internal/multiraft` persisted immutable catalog snapshots, range-scoped leader
hints, one bounded transport and a round-robin logical scheduler without
changing the Raft core.

Phase 3 exposes only local replica inspection. Followers may be stale, and the
leader path has no ReadIndex or lease proof. The architecture's unbounded-clock-
skew assumption remains unchanged: no leader-lease safety claim was introduced.

### 2.2 The control plane

```text
        ┌──────────────────────────────────────────────┐
        │              Per-range telemetry             │
        │  read/write QPS, bytes, size, latency        │
        │  percentiles, leader count, replication lag  │
        └──────────────────────┬───────────────────────┘
                               │ periodic aggregation
                               ▼
        ┌──────────────────────────────────────────────┐
        │                 Rebalancer                   │
        │                                              │
        │   detect ──► propose ──► validate ──► admit  │
        │  (hotspot,   (candidate  (safety     (rate   │
        │   imbalance)  action)     constraints) limit)│
        └──────────────────────┬───────────────────────┘
                               │ admitted plan
                               ▼
        ┌──────────────────────────────────────────────┐
        │             Control-plane operations         │
        │                                              │
        │  SplitRange   MoveReplica   TransferLeader   │
        │  AddReplica   RemoveReplica                  │
        │                                              │
        │  each is a restartable state machine whose   │
        │  state is itself replicated via Raft         │
        └──────────────────────┬───────────────────────┘
                               │
                               ▼
        ┌──────────────────────────────────────────────┐
        │      Range / Raft group reconfiguration      │
        │   (data plane keeps serving throughout)      │
        └──────────────────────────────────────────────┘
```

Two properties of this loop are load-bearing:

1. **The decision logic is deterministic.** Given the same telemetry snapshot
   and the same configuration, the rebalancer proposes the same plan. This is
   what makes it testable: a unit test feeds a synthetic snapshot and asserts
   on the exact proposed action, with no cluster and no timing.
2. **The validator is the only path to execution.** Proposals are advisory
   regardless of origin. Anything that cannot satisfy the safety constraints in
   §9 is rejected. The optional AI operator (Phase 12) plugs in as another
   proposal source upstream of the validator, never downstream of it.

---

## 3. Storage engine

An LSM tree, implemented in this repository. Rationale versus a B+ tree is in
[ADR-0001](design-decisions/0001-lsm-tree-over-b-tree.md); the full design is in
[storage-engine.md](storage-engine.md).

```text
   PUT(key, value)
        │
        ▼
   ┌──────────┐   append + optional fsync
   │   WAL    │   every record carries a CRC32C checksum
   └────┬─────┘
        │
        ▼
   ┌──────────┐   sorted in-memory map, bounded by size
   │ MemTable │
   └────┬─────┘
        │ sealed when full (writes continue into a fresh MemTable)
        ▼
   ┌──────────────────┐
   │ Immutable MemTable│  ──flush──► durable SSTable ──Manifest fsync──► L0
   └──────────────────┘
                                          │
                                     compaction
                                          ▼
                                  SSTable in L1, L2, ...
```

Phase 1J reads capture one explicitly published sequence and one coherent
active/immutable/Version view. Assigned sequences do not become readable until
their complete WAL-durable MemTable batch is applied and its final sequence is
published once. Point lookup considers every source that can contain a
candidate: active, all immutables, every overlapping L0 table and one
binary-range-selected table per non-overlapping higher level. The highest sequence wins globally; a
tombstone shadows older values. Scan heap-merges the same sources and collapses
versions by user key. User-key Bloom filters skip definite-negative SSTables,
and a bounded leased reader cache amortizes eager complete-file validation.

The engine owns its on-disk format. Every structure — WAL record framing,
SSTable blocks, the index, the Bloom filter, the footer, the manifest — is
specified byte-for-byte in [storage-engine.md](storage-engine.md) §5 and
carries a version number so the format can evolve.

Phase 1H's first compactor is a single explicit executor. It selects an
immutable-Version plan, performs deterministic bounded heap merge outside the
VersionSet lock, then revalidates and installs through one Manifest edit. It is
strictly version-preserving: without MVCC visibility evidence it drops neither
old versions nor tombstones. Phase 1J may delete obsolete inputs only after an
exclusive Engine operation-lifetime lock proves no old Version reader or
in-flight compaction can still reference them.

The integrated Engine orchestrates rather than duplicates these units. Flush
installation may briefly make an immutable and its identically numbered L0
file both visible, but removes the immutable only after Manifest durability and
Version publication, so no read gap exists. Compaction similarly publishes one
new immutable Version while readers that captured the old Version finish on
retained inputs. Phase 1K's bounded SSTable cache retains eager validation and
uses explicit leases so reclamation cannot close or unlink a borrowed reader.
The single active WAL has
whole-file candidate inspection but cannot be physically reclaimed until a
future closed-segment lifecycle exists.

---

## 4. Consensus

Raft, implemented in this repository. Not an external library: the failure
handling *is* the project, and delegating it would mean the interesting bugs
live somewhere else.

The implementation is a synchronous, single-owner event-driven state machine.
Each `Tick`, message or proposal deterministically changes protocol state;
safety-dependent changes are atomically saved through an injected Raft Store
before dependent messages are returned, and committed entries apply through an
injected state machine. The core performs no direct network I/O and reads no
wall clock. A driver maps [`clock.Clock`](../internal/clock/clock.go) progress
to logical ticks. Consequences:

- Election, replication and log-conflict resolution can be tested with no
  network, no disk and no real time, in microseconds per scenario.
- A failing chaos seed replays exactly, because the entire schedule — message
  delivery order, timer firing, randomised election timeouts — is derived from
  that seed.
- Persistence, transport and the state machine are injected, so the same
  protocol code is exercised by both unit tests and real clusters.

Phase 2 persists `currentTerm`, `votedFor`, snapshots and the replicated log
through a Raft-specific Store, independently of the Phase 1 data WAL. The
former is the consensus durability authority. Phase 3 carries committed
commands into a replicated-mode LSM without a data WAL, using the Raft index as
logical sequence and a distinct Manifest durable-applied frontier; see
[ADR-0015](design-decisions/0015-raft-to-lsm-replicated-state-machine.md).

---

## 5. Multi-Raft

A single Raft group for the whole database would serialise every write in the
cluster through one leader and make the "move load away from a hot node"
question unanswerable. RivetDB partitions the keyspace into contiguous,
ordered ranges, each replicated by its own independent Raft group.

```text
keyspace:   [-inf ──────── g ──────── p ──────── +inf)
             range 1        range 2     range 3

node A:  R1 leader     R2 follower   R3 follower
node B:  R1 follower   R2 leader     R3 follower
node C:  R1 follower   R2 follower   R3 leader
```

A range descriptor is the authoritative statement of "who serves these keys":

```text
RangeDescriptor {
    RangeID     uint64        stable identity, never reused
    StartKey    KeyBound      inclusive; explicit -infinity allowed
    EndKey      KeyBound      exclusive; explicit +infinity allowed
    Replicas    []ReplicaDesc (distinct node and replica identity; voters only)
    Generation  uint64        bumped on every membership or bounds change
}
```

`Generation` is what makes stale routing safe. A request carries the generation
the client believed; a replica that sees an older generation rejects it with
the current descriptor attached, and the client retries against the truth. This
turns "the client had a stale map" from a correctness problem into a
one-round-trip cost.

Phase 4 persists the complete static descriptor catalog independently on every
participating node using a bounded checksummed binary format and crash-safe
file/directory fsync publication. The default catalog covers the full keyspace
without overlap or gaps. Directories are never ownership authority. Dynamic
metadata consensus remains deferred.

Range partitioning rather than hash partitioning is chosen so that `SCAN` is a
local operation and so that a hot region of the keyspace can be *split* — the
central move of this project. The tradeoff (sequential keys create a hot range)
is exactly the problem the rebalancer addresses. See
[ADR-0002](design-decisions/0002-range-partitioning-over-hashing.md).

Raft groups on a node share one bounded logical message queue. Deterministic
simulation uses a round-robin scheduler; the physical Node runtime uses a fixed
four-worker fair pool with per-range FIFO queues and at most one active item per
range. Neither starts an OS timer or scheduler goroutine per group. Each LSM
retains its one bounded flush worker because storage lifecycle is range-local.

---

## 6. Routing

The Phase 4 router turns a key into a range-scoped leader candidate. Its
catalog is immutable; lookups binary-search sorted interval ends in `O(log R)`
and return owned descriptor bytes. Explicit catalog replacement constructs a
new snapshot rather than mutating descriptors in place.

Staleness is the normal case, not an error case. Four things can be out of
date, and each has a defined recovery:

| Stale fact | Detected by | Recovery |
|---|---|---|
| Leader moved | `NotLeader` response carrying the current leader hint | retry against the hint |
| Range split | descriptor generation mismatch | return current descriptor; refresh is deferred |
| Replica moved | `RangeNotFound` on the addressed node | refetch descriptor |
| Whole map stale | repeated misses | full range-map refresh |

Phase 4 retries only pre-admission `NotLeader` responses, over a bounded
candidate count and bounded logical scheduler work, while respecting context
cancellation. An admitted command is never automatically retried because
client deduplication is not implemented and its outcome can be ambiguous.
`ErrLeaderUnknown`, `ErrRangeNotFound`, and generation-bearing
`ErrStaleRange` remain explicit.

---

## 7. Transactions

### 7.1 MVCC

Every write produces a new version rather than overwriting. Keys are stored as
`(user key, commit timestamp)` sorted so that versions of one key are adjacent
and ordered newest-first, which makes "the visible version at timestamp T" a
single seek.

```text
account:123 @ ts=170  ──►  {balance: 40}
account:123 @ ts=130  ──►  {balance: 90}
account:123 @ ts=100  ──►  {balance: 50}

read at ts=150 sees {balance: 90}
```

A transaction reads at a fixed timestamp for its whole lifetime, so it observes
a consistent snapshot regardless of concurrent commits.

Uncommitted writes are stored as **write intents**: a provisional version
tagged with the writing transaction's ID and a pointer to its transaction
record. A reader that encounters an intent must resolve it — look up the
transaction record, and either treat the write as committed, ignore it, or wait.
This is how the system avoids a separate lock table that would itself need to
be replicated and recovered.

Garbage collection removes versions older than the oldest active read
timestamp. Until that GC threshold is implemented, storage grows without bound;
this is tracked as a known limitation rather than assumed away.

### 7.2 Isolation

The initial target is **snapshot isolation**. That means write skew is
*permitted*. RivetDB will not describe itself as serializable unless there is a
mechanical test demonstrating it — the plan is a history-generating harness
plus a checker, and the claim follows the evidence rather than the intention.
The precise anomaly table — which anomalies are prevented, which are permitted,
each backed by a test — is written as `docs/transactions.md` when Phase 6
begins.

### 7.3 Cross-range transactions

A transaction touching keys in more than one range cannot be committed by one
Raft group. RivetDB uses two-phase commit where the transaction record is
itself a replicated key, living in the range of the transaction's first written
key:

```text
coordinator                participant ranges
    │
    │ 1. write intents (each replicated by its own Raft group)
    ├──────────────────────────►  R4:  intent on account:A
    ├──────────────────────────►  R11: intent on account:B
    │
    │ 2. commit: flip the transaction record to COMMITTED
    │    ── this single replicated write is the atomic commit point ──
    │
    │ 3. resolve intents (asynchronous, idempotent, restartable)
    ├──────────────────────────►  R4:  intent → committed version
    └──────────────────────────►  R11: intent → committed version
```

The property that makes this recoverable: **the transaction record is the sole
source of truth, and it is replicated.** A coordinator that crashes after step 2
has already committed; step 3 is cleanup that any reader can perform on
encountering an intent. A coordinator that crashes before step 2 leaves intents
that will be aborted when someone notices the record is missing or expired.
There is no window in which the outcome depends on a process that is gone.

Step 3 must be idempotent because it will be retried by crash recovery, by
concurrent readers, and by duplicate RPC delivery simultaneously.

---

## 8. Online reconfiguration

Both reconfiguration operations are long-running, restartable state machines
whose current step is durably recorded before it is attempted. Crash recovery
resumes from the recorded step; each step is idempotent so re-running it is
safe.

### 8.1 Range split

```text
[a, z)  ──►  [a, m)  +  [m, z)
```

The split is a single Raft entry in the original range's log. That is what
makes it atomic with respect to concurrent traffic: every write is ordered
before or after the split entry, never during it. Applying the entry creates
the right-hand range's state, updates both descriptors, and bumps both
generations. Requests that raced the split arrive with the old generation and
are redirected.

The split key is chosen by sampling the range's key distribution, so that the
result is two ranges of comparable load rather than of comparable width.

### 8.2 Replica migration

Migration is add-then-remove, never remove-then-add, because the latter drops
below the replication factor and can lose quorum if a second node fails during
the window:

```text
{A, B, C}                       replication factor 3, quorum 2
   │ add D as a non-voting learner   → quorum still 2 of {A,B,C}
{A, B, C, D*}
   │ stream a snapshot to D, then catch it up from the log
   │ promote D to voter              → quorum 3 of {A,B,C,D}
{A, B, C, D}
   │ transfer leadership away from A if A leads
   │ remove A                        → quorum 2 of {B,C,D}
{B, C, D}
```

Learners receive data without voting, so a slow new replica delays the
migration but never the cluster.

---

## 9. Rebalancing

The controller runs a fixed pipeline on each cycle:

```text
collect telemetry  →  detect  →  propose  →  validate  →  admit  →  execute  →  verify
```

Detection covers hot ranges (QPS or bytes far above the cluster median),
overloaded nodes, storage imbalance, leadership concentration, and replica
count imbalance. Proposals are drawn from a fixed vocabulary: `SplitRange`,
`MoveReplica`, `AddReplica`, `RemoveReplica`, `TransferLeader`.

Validation is a hard gate. A proposal is rejected unless all of these hold:

- the replication factor remains satisfied at every intermediate step;
- quorum is preserved at every intermediate step;
- the target node has capacity headroom below the configured threshold;
- anti-affinity holds — no two replicas of one range on the same node;
- the number of concurrent migrations is under the configured maximum;
- the range is outside its cooldown window since its last operation.

Cooldown and concurrency limits exist because the failure mode of an automatic
rebalancer is not a bad single decision — it is oscillation, where moving load
off a node makes another node hot and the controller thrashes. Rate limiting
turns that from an outage into slow convergence.

---

## 10. Concurrency model

Ownership is stated per subsystem and enforced by structure, not by convention.
The general shape:

- **Serialised state machines.** Each Raft/range instance serializes Tick,
  Step, proposal and apply behind its own lock. A shared external scheduler
  provides bounded work units; it does not require a goroutine per group.
- **Immutable snapshots for read-mostly data.** The range map and the storage
  engine's version set are immutable values swapped atomically. Readers take a
  reference and are never blocked by a writer.
- **Bounded queues everywhere.** Every inter-goroutine channel has a fixed
  capacity, and a full queue applies backpressure rather than growing. An
  unbounded queue converts an overload into an out-of-memory crash, which is a
  much worse failure than a rejected request.
- **Storage flush ownership.** Phase 1F serializes sequence assignment,
  synchronous WAL append, atomic MemTable batch apply and rotation with one
  admission token. One separate worker owns the bounded FIFO immutable queue;
  it performs Phase 1D publication without holding write admission, retains a
  failed head, and joins on shutdown. Phase 1G gives that worker a narrow
  durable file allocator and table installer: physical publication precedes
  the AddFile Manifest fsync, and only then is the immutable Version published.
  The interface remains engine-scoped and does not foreclose a future shared
  WAL across ranges.
- **Visibility and reclamation ownership.** Phase 1J separates assigned from
  published storage sequences. Get and Scan capture the published high-water
  once. Every Engine operation that may use an immutable Version or SSTable
  holds the shared operation lock for that lifetime; obsolete-table deletion
  takes it exclusively, rechecks the current Version and evicts any cached
  identity before unlinking. A borrowed cache lease retains the file.
- **Explicit lifecycle.** Every component that starts goroutines exposes
  `Close`, cancels a context, and waits for its goroutines to exit.
  [`testutil.NoLeaks`](../internal/testutil/leak.go) enforces this in tests: a
  node hosting hundreds of ranges cannot afford to leak a goroutine per
  teardown.

Everything runs under `go test -race` in CI.

---

## 11. Failure model

RivetDB assumes:

- **Crash-stop with recovery.** Nodes may halt at any instruction and restart
  with their disk intact. Byzantine behaviour is out of scope.
- **An asynchronous network.** Messages may be dropped, delayed, reordered or
  duplicated. Partitions may be asymmetric — A reaching B while B cannot reach
  A — which is the case that produces stale leaders.
- **Unsynchronised clocks.** No bound on skew is assumed. Timers are used for
  liveness (elections, timeouts) but never for safety.
- **Partially reliable disks.** Torn writes and bit corruption are detected via
  checksums. RivetDB detects corruption; it does not repair it locally, and
  recovery from a corrupt replica is to rebuild it from a peer.

What is *not* assumed, and therefore must be tested: that any of this is
handled correctly. The fault-injection framework, chaos campaigns and invariant
checks in [correctness.md](correctness.md) exist to produce that evidence.

---

## 12. Observability

Prometheus-compatible metrics for the database (throughput, errors, latency
histograms), storage (MemTable size, SSTable counts, compaction bytes and
duration, Bloom filter hit rate), Raft (term, role, leader changes, commit and
applied index, replication lag), transactions (started, committed, aborted,
conflicts, retries) and the rebalancer (hotspots detected, moves proposed,
admitted, succeeded, failed).

Logs are structured throughout, via [`internal/rlog`](../internal/rlog). The
attribute keys `node`, `range`, `term`, `index` and `txn` are canonical
constants so that a log query can follow one range across every node in the
cluster — which is the query you actually need when debugging a failed split.

---

## 13. Repository layout

Only directories with code in them exist. The rest appear as their phase
begins.

```text
internal/clock/       time abstraction: real and deterministic mock
internal/invariant/   safety assertions with named violations
internal/rlog/        structured logging and a test recorder
internal/testutil/    seeds and explicit failure-corpus promotion, leak detection, polling
internal/storage/     key/batch, WAL, MemTable, SSTable writer/reader and flush pipeline
internal/raft/        deterministic Raft core, stores and simulator
internal/replicatedrange/ one durable Raft-to-LSM replica
internal/multiraft/   static catalog, node registry, shared transport/scheduler and router
docs/                 this document, invariants, roadmap, ADRs
```

Planned, in roadmap order: `internal/txn`, `internal/migration`,
`internal/rebalance`, `internal/telemetry`, `internal/server`, plus `cmd/`,
`api/proto`, `tests/` and `benchmarks/`.

---

## 14. Open questions

Recorded here rather than silently deferred:

1. **Timestamp allocation (resolved for MVCC).** ADR-0017 chooses range-scoped
   48/16 HLC state over a cluster oracle. Phase 6 must still define how a
   transaction-level read and commit timestamp is selected across participants.
2. **Read path.** Routing every read through Raft is obviously correct but
   costs a round trip. ReadIndex avoids the log write; leader leases avoid the
   round trip entirely but make safety depend on clock bounds, which §11 says
   are not assumed. Phase 3 deliberately exposes only stale-capable local
   inspection. Phase 4 exposes routed mutations only; the distributed read
   choice remains open.
3. **Dynamic range metadata authority.** Phase 4 uses an identical persisted
   static bootstrap catalog on every node. A future dedicated meta-range,
   distributed catalog, or external placement driver remains undecided.
4. **Split key selection.** Sampling gives a size-balanced split; a
   load-balanced split needs per-key access statistics, which cost memory
   proportional to the working set. Undecided.
### Phase 5 replicated MVCC layer

Each range now optionally composes a range-scoped HLC, timestamped command
codec, historical Engine reads, applied MVCC watermark and read-only snapshot
registry. The shared physical clock provider is not mutable timestamp
authority. Raft index, durable applied Raft frontier, volatile MVCC watermark
and durable MVCC watermark are distinct. Timestamps are comparable across
ranges, but there is no multi-range atomic snapshot or transaction protocol.
