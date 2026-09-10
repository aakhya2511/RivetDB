# RivetDB Invariants

An invariant here is a property that must hold in every reachable state. If one
is violated, RivetDB has lost correctness — the right response is to stop, not
to continue and hope.

This file is the contract the tests are written against. Each invariant has a
stable identifier, which is used in three places:

1. the `name` argument to [`invariant.Assert`](../internal/invariant/invariant.go),
   so a runtime violation names itself;
2. the test that demonstrates it, so "where is the test?" has an answer;
3. the chaos harness's post-run checks.

**Status column:**

- `foundation` — the invariant is about Phase 0 infrastructure and is enforced today.
- `planned (Phase N)` — the subsystem does not exist yet.

Nothing is marked verified until an implementation and a test exist. This file
is updated as part of the phase gate, not afterwards.

---

## Storage engine

| ID | Invariant | Status |
|---|---|---|
| STORAGE-1 | A write acknowledged in a durability mode that promises persistence is present after a crash at any point after the acknowledgement. | verified for WAL (Phase 1B, filesystem contract) |
| STORAGE-2 | Every WAL record's checksum is verified on replay. A record whose checksum fails is not applied. | verified (Phase 1B) |
| STORAGE-3 | A partially written trailing WAL record is truncated, not applied. A crash mid-append loses only the in-flight record, never an earlier one. | verified (Phase 1B) |
| STORAGE-4 | Keys within an SSTable are strictly ascending, with no duplicates of the same `(key, timestamp)` pair. | planned (Phase 1) |
| STORAGE-5 | A read returns the newest version of a key visible to it, considering the MemTable, the immutable MemTable and every SSTable level. | planned (Phase 1) |
| STORAGE-6 | Compaction never resurrects a deleted key: a tombstone is dropped only when no older version of that key can survive in any remaining file, and no live reader can be positioned before it. | planned (Phase 1) |
| STORAGE-7 | Compaction is value-preserving: for every key and every read timestamp, the value visible before compaction equals the value visible after. | planned (Phase 1) |
| STORAGE-8 | A Bloom filter never produces a false negative. A "not present" answer from the filter means the key is genuinely absent from that SSTable. | planned (Phase 1) |
| STORAGE-9 | The manifest describes exactly the set of SSTables that exist and are reachable. Recovery never opens a file absent from the manifest, and never misses one present in it. | planned (Phase 1) |
| STORAGE-10 | An iterator observes a fixed, consistent view of the engine for its whole lifetime. Concurrent flushes and compactions do not change what it yields. | planned (Phase 1) |
| STORAGE-11 | A file is made visible to readers only after its contents and its directory entry are durable. No reader ever observes a torn or partial SSTable. | planned (Phase 1) |
| STORAGE-12 | Different internal keys are ordered first and solely by lexicographic user-key bytes; sequence and kind cannot reverse the order of distinct user keys. | verified (pre-Phase-1) |
| STORAGE-13 | For one user key, a higher sequence number sorts before a lower sequence number. | verified (pre-Phase-1) |
| STORAGE-14 | Internal-key comparison is a strict total order: it is antisymmetric and transitive, and equality means user key, sequence and kind are all equal. | verified (pre-Phase-1) |
| STORAGE-15 | Every version of one user key is contiguous in internal-key order. | verified (pre-Phase-1) |
| STORAGE-16 | Range bounds are user keys, and no range boundary can split the internal versions of one logical user key. | design contract (pre-Phase-1) |
| STORAGE-17 | WAL recovery returns only complete, checksum-valid logical records, in original append order, and its valid-end offset is the maximal proven record boundary. | verified (Phase 1B) |
| STORAGE-18 | WAL decoding validates header integrity and physical/logical size bounds before trusting lengths or growing buffers. Malformed input cannot cause overflow or unbounded allocation. | verified (Phase 1B) |
| STORAGE-19 | A checksum failure, invalid format/type, impossible fragment sequence or nonzero padding is corruption and is never silently skipped or repaired as a truncated tail. | verified (Phase 1B) |
| STORAGE-20 | Explicit tail repair truncates only a rescanned, unchanged, structurally incomplete tail. Appending after repair cannot resurrect discarded bytes. | verified (Phase 1B) |
| STORAGE-21 | Concurrent WAL appends are serialized into complete, non-interleaved logical records. A writer that encounters an I/O failure cannot acknowledge later appends as healthy. | verified (Phase 1B) |
| STORAGE-22 | Write-batch encoding is unambiguous and all-or-nothing: sequence ranges cannot wrap, malformed batches return an error, and DELETE differs from PUT of an empty value. | verified (Phase 1A) |

STORAGE-12 through STORAGE-15 are enforced by
[`internal_key_test.go`](../internal/storage/internal_key_test.go), including
deterministic binary/prefix cases and seeded property tests. STORAGE-16 is a
contract for the range and split implementations in Phases 4 and 7; those
phases must add end-to-end enforcement tests before marking it verified.

STORAGE-1 through STORAGE-3 and STORAGE-17 through STORAGE-21 are enforced by
[`wal_test.go`](../internal/storage/wal/wal_test.go), including every-offset
truncation, systematic protected-byte corruption, real-file restart/repair and
injected write/sync/close failures. STORAGE-22 is enforced by
[`batch_test.go`](../internal/storage/batch_test.go), including malformed-length
and maximum-size cases.

## Raft

| ID | Invariant | Status |
|---|---|---|
| RAFT-1 | *Election safety.* At most one leader is elected per term. | planned (Phase 2) |
| RAFT-2 | *Leader append-only.* A leader never overwrites or deletes entries in its own log; it only appends. | planned (Phase 2) |
| RAFT-3 | *Log matching.* If two logs contain an entry with the same index and term, the logs are identical in every entry through that index. | planned (Phase 2) |
| RAFT-4 | *Leader completeness.* If an entry is committed in a term, it is present in the log of every leader of every later term. | planned (Phase 2) |
| RAFT-5 | *State machine safety.* If two nodes apply an entry at a given log index, it is the same entry. No node ever applies conflicting commands at the same index. | planned (Phase 2) |
| RAFT-6 | `currentTerm` never decreases at a node, across restarts included. | planned (Phase 2) |
| RAFT-7 | `commitIndex` never decreases; `appliedIndex ≤ commitIndex` always. | planned (Phase 2) |
| RAFT-8 | A node grants at most one vote per term, and that grant is durable before the response is sent. | planned (Phase 2) |
| RAFT-9 | A leader that has lost quorum cannot commit new entries, even before it learns it has lost quorum. | planned (Phase 2) |
| RAFT-10 | Persistent state (`currentTerm`, `votedFor`, log entries) is durable before any RPC response depending on it is sent. | planned (Phase 2) |
| RAFT-11 | Log compaction discards only entries covered by a durable snapshot, and a snapshot's applied index is never ahead of what it contains. | planned (Phase 2) |
| RAFT-12 | Installing a snapshot yields a state machine identical to applying every entry the snapshot covers. | planned (Phase 2) |

## Ranges and routing

| ID | Invariant | Status |
|---|---|---|
| RANGE-1 | Range key intervals partition the keyspace: the union of all `[start, end)` intervals covers it exactly, with no gap and no overlap. | planned (Phase 4) |
| RANGE-2 | Exactly one range is responsible for any given key at any moment. Ownership is never ambiguous, including during a split. | planned (Phase 4) |
| RANGE-3 | A range's `Generation` strictly increases and is bumped by every change to its bounds or replica set. | planned (Phase 4) |
| RANGE-4 | A request carrying a stale generation is rejected with the current descriptor, never served from stale state. | planned (Phase 4) |
| RANGE-5 | A range ID is never reused, including after the range is split or removed. | planned (Phase 4) |
| RANGE-6 | Every key a client can read was written to the range that currently owns it. Routing never silently sends a key to the wrong range. | planned (Phase 4) |

## MVCC

| ID | Invariant | Status |
|---|---|---|
| MVCC-1 | A read at timestamp `T` observes exactly the writes committed with a timestamp `≤ T`, and no others, for its whole lifetime. | planned (Phase 5) |
| MVCC-2 | Writes of an uncommitted transaction are invisible to every other transaction. | planned (Phase 5) |
| MVCC-3 | An aborted transaction leaves no externally visible state. Every intent it wrote is eventually removed. | planned (Phase 5) |
| MVCC-4 | Two concurrent transactions writing the same key cannot both commit. At least one aborts. | planned (Phase 5) |
| MVCC-5 | A committed version's timestamp is greater than the read timestamp of any transaction that observed the prior version and then wrote it. | planned (Phase 5) |
| MVCC-6 | Garbage collection never removes a version that a live read timestamp could observe. | planned (Phase 5) |
| MVCC-7 | A key has at most one write intent at a time. | planned (Phase 5) |

## Distributed transactions

| ID | Invariant | Status |
|---|---|---|
| TXN-1 | *Atomicity.* A transaction's writes are either all visible or none are, regardless of how many ranges it spans. | planned (Phase 6) |
| TXN-2 | The transaction record's state is the single source of truth for the outcome. No participant can reach a different conclusion. | planned (Phase 6) |
| TXN-3 | A transaction's outcome, once decided, never changes. A committed transaction is never later reported aborted, and vice versa. | planned (Phase 6) |
| TXN-4 | Every transaction operation is idempotent. Duplicate prepare, commit or abort messages produce the same result as one delivery. | planned (Phase 6) |
| TXN-5 | Recovery resolves every transaction left prepared by a crash, in bounded time, without operator intervention. | planned (Phase 6) |
| TXN-6 | A coordinator crash never leaves a transaction permanently undecided. | planned (Phase 6) |

## Split and migration

| ID | Invariant | Status |
|---|---|---|
| SPLIT-1 | A split is atomic with respect to concurrent traffic: every write is ordered strictly before or strictly after it. | planned (Phase 7) |
| SPLIT-2 | Every key readable before a split is readable after it, with the same value, from exactly one of the resulting ranges. | planned (Phase 7) |
| SPLIT-3 | A crash at any point during a split leaves a state that recovery completes or abandons cleanly. There is no permanently half-split range. | planned (Phase 7) |
| SPLIT-4 | The resulting ranges' bounds partition the original range's bounds exactly. | planned (Phase 7) |
| MIGRATE-1 | The replica set never drops below the configured replication factor at any intermediate step of a migration. | planned (Phase 8) |
| MIGRATE-2 | Quorum is preserved at every intermediate step, including if any single node fails mid-migration. | planned (Phase 8) |
| MIGRATE-3 | Data remains readable throughout a migration. No window exists in which a committed key is unreachable. | planned (Phase 8) |
| MIGRATE-4 | Repeating a control-plane command is safe. A duplicated `MoveReplica` produces one move, not two. | planned (Phase 8) |
| MIGRATE-5 | A migration interrupted by a crash or a leader change either completes or rolls back; it does not stall indefinitely. | planned (Phase 8) |
| MIGRATE-6 | No two replicas of the same range are placed on the same node. | planned (Phase 8) |

## Rebalancing

| ID | Invariant | Status |
|---|---|---|
| BALANCE-1 | The controller is deterministic: identical telemetry and configuration yield an identical plan. | planned (Phase 9) |
| BALANCE-2 | No proposal executes without passing the safety validator, regardless of its source. | planned (Phase 9) |
| BALANCE-3 | Concurrent migrations never exceed the configured maximum. | planned (Phase 9) |
| BALANCE-4 | A range that has just been reconfigured is not reconfigured again before its cooldown expires. | planned (Phase 9) |
| BALANCE-5 | The controller does not oscillate: a stable workload reaches a fixed point and stops proposing actions. | planned (Phase 9) |

## Foundation

These concern the Phase 0 infrastructure and are enforced today.

| ID | Invariant | Status | Test |
|---|---|---|---|
| FOUND-1 | A recorded seed replays the identical random stream, so a persisted failing seed reproduces its failure. | foundation | [`TestRandIsReproducible`](../internal/testutil/testutil_test.go) |
| FOUND-2 | A deliberately promoted failing seed is persisted to the corpus and is not duplicated on repeat promotion. Ordinary test and CI execution never modifies the corpus. | foundation | [`TestPromoteSeedAppendsAndDeduplicates`](../internal/testutil/testutil_internal_test.go) |
| FOUND-3 | A corpus path derived from a test name always resolves inside `testdata/seeds`, whatever the name contains. | foundation | [`TestSanitizeTestNameCannotEscapeCorpusDir`](../internal/testutil/testutil_internal_test.go) |
| FOUND-4 | Mock clock timers fire in deadline order, and `Now` during a fire is never behind that timer's deadline. | foundation | [`TestMockFiresInDeadlineOrder`](../internal/clock/mock_test.go), [`TestMockNowDuringFireIsDeadline`](../internal/clock/mock_test.go) |
| FOUND-5 | Mock clock time advances only when a test advances it. | foundation | [`TestMockStartsAtEpochAndDoesNotDrift`](../internal/clock/mock_test.go) |
| FOUND-6 | A goroutine started during a test and still running at its end is reported. | foundation | [`TestLeakedSinceDetectsAndClears`](../internal/testutil/testutil_internal_test.go) |
| FOUND-7 | An invariant violation panics with a typed, named `*Violation` so a chaos run can attribute the crash. | foundation | [`TestAssertPanicsWithTypedViolation`](../internal/invariant/invariant_test.go) |

---

## How these are checked

Three complementary mechanisms, because no single one is sufficient:

1. **Runtime assertions.** Invariants cheap enough to check on a control path
   are asserted in production code via `invariant.Assert`, named with the IDs
   above. Structural checks that are O(n) in the data — SSTable ordering, range
   bound tiling, Raft log continuity — are guarded by `invariant.Expensive()`
   and enabled in tests and chaos runs.

2. **Targeted tests.** Each invariant has at least one test that constructs the
   specific situation that would violate it, including the crash and partition
   cases. A test that only exercises the happy path is not evidence for an
   invariant about failures.

3. **History checking.** The chaos harness records every client invocation and
   response with timestamps and checks the resulting history against the
   consistency model RivetDB actually claims. The scope of what is and is not
   checked is stated explicitly in [correctness.md](correctness.md); an unchecked
   property is listed as unchecked rather than assumed.
