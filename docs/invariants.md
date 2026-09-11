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
| STORAGE-4 | Keys within an SSTable are strictly ascending, with no duplicate internal keys. | verified for individual SSTables (Phase 1E) |
| STORAGE-5 | A read returns the newest version of a key visible to it, considering the MemTable, the immutable MemTable and every SSTable level. | verified for latest-state reads (Phase 1I) |
| STORAGE-6 | Compaction never resurrects a deleted key: a tombstone is dropped only when no older version of that key can survive in any remaining file, and no live reader can be positioned before it. | verified by retaining all tombstones (Phase 1H/1I) |
| STORAGE-7 | Compaction is value-preserving: for every key and every read timestamp, the value visible before compaction equals the value visible after. | verified by exact version preservation (Phase 1H) |
| STORAGE-8 | A Bloom filter never produces a false negative. A "not present" answer from the filter means the key is genuinely absent from that SSTable. | verified (Phase 1K) |
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
| STORAGE-23 | MemTable level-zero iteration is strictly ordered by `CompareInternal`, and every higher skip-list level is an ordered subsequence. | verified (Phase 1C) |
| STORAGE-24 | The MemTable preserves every distinct internal-key version contiguously; only an exactly comparator-equal key replaces an entry. | verified (Phase 1C) |
| STORAGE-25 | A MemTable owns inserted key and value bytes, and returned entries do not expose its mutable storage. | verified (Phase 1C) |
| STORAGE-26 | Freeze is permanent: an insert racing with freeze is wholly accepted before it or rejected, and no insert succeeds after freeze returns. | verified (Phase 1C) |
| STORAGE-27 | MemTable seek returns the `CompareInternal` lower bound, including for arbitrary binary and prefix-related keys. | verified (Phase 1C) |
| STORAGE-28 | MemTable user-key range bounds are half-open and include either every version of a logical key or none. | verified (Phase 1C) |
| STORAGE-29 | MemTable approximate size is monotonic while mutable, stable after freeze, and saturates rather than overflowing. | verified (Phase 1C) |
| STORAGE-30 | MemTable reads, writes, iterator snapshots and freeze obey their documented linearizable synchronization contract without exposing partial mutations. | verified (Phase 1C) |
| STORAGE-31 | SSTable entries and full block-boundary keys are strictly ordered by `CompareInternal`; exact duplicates and out-of-order input are rejected. | verified (Phase 1D) |
| STORAGE-32 | Every data-block entry reconstructs one unambiguous encoded internal key and value from canonical bounded lengths and restart state. | verified (Phase 1D) |
| STORAGE-33 | Every independently readable SSTable block authenticates its payload, compression, kind and block version with CRC32C. | verified (Phase 1D) |
| STORAGE-34 | Every SSTable index handle names exactly one complete data block, and index keys equal those blocks' full last internal keys. | verified (Phase 1D) |
| STORAGE-35 | The fixed SSTable footer authenticates its fields and names only non-overlapping, correctly typed in-file regions with no unexplained bytes. | verified (Phase 1D) |
| STORAGE-36 | Identical ordered entries and writer options produce byte-identical SSTables. | verified (Phase 1D) |
| STORAGE-37 | A truncated or partially written SSTable is corruption and is never treated as a recoverable WAL-style tail. | verified (Phase 1D) |
| STORAGE-38 | SSTable publication exposes the final filename only after the complete file is fsynced, then fsyncs the containing directory; any I/O failure poisons the writer. | verified (Phase 1D) |
| STORAGE-39 | All versions of one user key retain authoritative order, while table range metadata remains expressed in decoded user keys and encodes no owner range ID. | verified (Phase 1D) |
| STORAGE-40 | An SSTable reader validates physical bounds, block envelopes and checksums before decoding or trusting persistent payload bytes. | verified (Phase 1E) |
| STORAGE-41 | SSTable Seek returns exactly the first internal key not less than its target under `CompareInternal`; exact Get succeeds if and only if that key is comparator-equal. | verified (Phase 1E) |
| STORAGE-42 | SSTable candidate lookup returns the newest version of the requested user key whose sequence is at most the target, preserving its value or deletion kind. | verified (Phase 1E) |
| STORAGE-43 | SSTable full iteration yields every stored internal entry exactly once in authoritative order; user-key range iteration yields exactly all versions in its half-open bounds. | verified (Phase 1E) |
| STORAGE-44 | Corrupt handles, lengths, canonical varints, restart points, block types and checksums cannot panic, overflow, loop without bound or drive allocation beyond format/resource limits. | verified (Phase 1E) |
| STORAGE-45 | Every trusted index boundary equals its data block's reconstructed last key, and metadata counts and bounds equal the fully validated data stream. | verified (Phase 1E) |
| STORAGE-46 | Tombstones are returned as stored entries and are never silently converted to absence by the SSTable reader. | verified (Phase 1E) |
| STORAGE-47 | An SSTable reader never modifies, skips or repairs corrupt immutable bytes; corruption and I/O failures are surfaced to its caller. | verified (Phase 1E) |
| STORAGE-48 | A write acknowledged by the Phase 1F pipeline crossed the synchronous WAL durability boundary before becoming visible in its MemTable. | verified (Phase 1F) |
| STORAGE-49 | Every accepted batch is applied wholly to exactly one active MemTable generation under one authoritative contiguous sequence assignment. | verified (Phase 1F) |
| STORAGE-50 | Exactly one mutable active MemTable exists while writes are accepted; rotation atomically freezes it, installs one successor and queues the frozen generation. | verified (Phase 1F) |
| STORAGE-51 | A frozen MemTable never becomes mutable, and a single immutable generation cannot have two concurrent or successful flushes. | verified (Phase 1F) |
| STORAGE-52 | A successful flush SSTable contains exactly every immutable MemTable entry once in `CompareInternal` order, including versions and tombstones. | verified (Phase 1F) |
| STORAGE-53 | An immutable generation remains live until Phase 1D durable publication and Phase 1E exact-content validation succeed. | verified (Phase 1F) |
| STORAGE-54 | Flush failure preserves immutable state, stops later FIFO flushes and is surfaced; ambiguous final files are never automatically deleted or overwritten. | verified (Phase 1F) |
| STORAGE-55 | The immutable backlog is bounded, and waiting for capacity is cancellable only before WAL append without busy waiting. | verified (Phase 1F) |
| STORAGE-56 | Phase 1F never deletes or truncates WAL data because no manifest yet authorizes reclamation. | verified (Phase 1F) |
| STORAGE-57 | Concurrent writes and rotations assign every acknowledged batch to exactly one monotonically ordered generation and non-overlapping sequence range. | verified (Phase 1F) |
| STORAGE-58 | Shutdown stops and joins the flush worker, closes the WAL and leaves every acknowledged write recoverable from WAL or a retained representation. | verified (Phase 1F) |
| STORAGE-59 | WAL replay validates and atomically reapplies complete batches through the same MemTable application contract, without interpreting flushed-state authority. | verified (Phase 1F) |
| STORAGE-60 | Only SSTables referenced by the Version recovered from CURRENT's Manifest are logically live; a physically valid unlisted table remains an orphan. | verified (Phase 1G) |
| STORAGE-61 | Every live file number is unique, and durable `NextFileNumber` is greater than every live, orphan or temporary SSTable number that may exist. | verified (Phase 1G) |
| STORAGE-62 | `LastSequence` never decreases, and recovery never reuses a sequence present in the Manifest, retained WAL or a validated live SSTable. | verified (Phase 1G) |
| STORAGE-63 | A VersionEdit either validates and installs every field atomically or leaves the current immutable Version unchanged. | verified (Phase 1G) |
| STORAGE-64 | Manifest corruption, unsupported framing and checksum-valid semantic corruption are surfaced; only a provably incomplete final record may recover as a complete prefix. | verified (Phase 1G) |
| STORAGE-65 | An SSTable becomes logically installed only after its AddFile VersionEdit is fsynced; failure before that point retains the immutable/WAL and leaves the table orphaned. | verified (Phase 1G) |
| STORAGE-66 | The inclusive replay frontier advances only across a contiguous sequence prefix represented by authoritative installed flush spans; a gap stops advancement. | verified (Phase 1G) |
| STORAGE-67 | The replay frontier never decreases, survives restart exactly and never splits an atomic WAL batch during replay. | verified (Phase 1G) |
| STORAGE-68 | A missing, corrupt or metadata-mismatched live SSTable causes metadata recovery to fail rather than silently changing the authoritative Version. | verified (Phase 1G) |
| STORAGE-69 | CURRENT names exactly one basename-valid Manifest and is replaced by temp-file fsync, rename and containing-directory fsync. | verified (Phase 1G) |
| STORAGE-70 | Manifest replacement never deletes or invalidates the old recoverable Manifest before the new CURRENT switch is durable; ambiguous switches retain both candidates. | verified (Phase 1G) |
| STORAGE-71 | Manifest replay and Version ordering are deterministic and independent of map iteration order. | verified (Phase 1G) |
| STORAGE-72 | Physical WAL files are retained in Phase 1G even when the durable replay frontier proves a prefix logically redundant. | verified (Phase 1G) |
| STORAGE-73 | Phase 1H compaction preserves the exact multiset of internal entries, including every version and tombstone; unrepresentable exact duplicates fail without replacement. | verified (Phase 1H) |
| STORAGE-74 | Merge output is globally ordered only by `storage.CompareInternal`; physical tie-breakers do not change logical ordering. | verified (Phase 1H) |
| STORAGE-75 | Compaction overlap closure uses logical user-key ranges and includes every transitively overlapping source and target table. | verified (Phase 1H) |
| STORAGE-76 | Compaction output tables in L1+ are ordered and non-overlapping, and no logical user-key version group is split at a target-size boundary. | verified (Phase 1H) |
| STORAGE-77 | One atomic VersionEdit deletes every compaction input and adds every output; no partial logical replacement is installable. | verified (Phase 1H) |
| STORAGE-78 | Before replacement Manifest durability inputs remain authoritative; afterward outputs are authoritative and inputs are only logically obsolete. | verified (Phase 1H) |
| STORAGE-79 | Durable outputs from a failed or pre-install compaction remain unlisted orphans and never displace live inputs. | verified (Phase 1H) |
| STORAGE-80 | A compaction plan whose input is no longer live with identical metadata cannot install. | verified (Phase 1H) |
| STORAGE-81 | Compaction does not change or infer WAL replay coverage from output sequence minima or maxima. | verified (Phase 1H) |
| STORAGE-82 | Compaction input corruption or any output failure aborts the logical replacement and is never skipped. | verified (Phase 1H) |
| STORAGE-83 | Compaction output file numbers come only from the durable VersionSet allocator; failed attempts may burn numbers but never reuse them. | verified (Phase 1H) |
| STORAGE-84 | Phase 1H does not physically delete obsolete input SSTables without an explicit immutable-Version/read-lifetime proof. | verified (Phase 1H) |
| STORAGE-85 | Engine recovery applies required durable WAL batches directly to one recovered MemTable and never appends them again; assignment resumes above every durable sequence. | verified (Phase 1I) |
| STORAGE-86 | Every Engine Get and Scan captures one sequence boundary, one active/immutable membership snapshot and one immutable Version for the operation. | verified (Phase 1I) |
| STORAGE-87 | Get resolves the greatest sequence not above its boundary across active, every immutable, every overlapping L0 table and at most one range-selected table per non-overlapping higher level. | verified (Phase 1I) |
| STORAGE-88 | A tombstone at the greatest visible sequence suppresses every older value across every source and maps to public not-found without erasing the stored tombstone. | verified (Phase 1I) |
| STORAGE-89 | Scan merges all relevant authoritative sources in internal-key order, emits at most one current value per user key, omits newest tombstones and obeys half-open user-key bounds. | verified (Phase 1I) |
| STORAGE-90 | Rotation and flush installation have no read-visibility gap: the old MemTable remains visible until its durable SSTable is authoritative in the Version. | verified (Phase 1I) |
| STORAGE-91 | A read holding an old immutable Version can finish against retained compaction inputs while new reads use the atomically installed output Version. | verified (Phase 1I) |
| STORAGE-92 | Valid orphan SSTables and stale temporary files never participate in Get or Scan; missing, corrupt or metadata-mismatched live tables prevent Open. | verified (Phase 1I) |
| STORAGE-93 | Contradictory same-sequence candidates and exact duplicates across different authoritative identities are corruption; only immutable-to-its-installed-file handoff overlap is accepted. | verified (Phase 1I) |
| STORAGE-94 | L0 reads account for arbitrary overlap, while higher-level point lookup uses non-overlapping decoded user-key ranges rather than internal-key bytes. | verified (Phase 1I) |
| STORAGE-95 | Successful Put/Delete acknowledgement remains WAL-before-MemTable and is immediately visible to subsequent operations on that Engine. | verified (Phase 1I) |
| STORAGE-96 | Engine shutdown rejects later operations, joins the flush worker and preserves an unflushed active MemTable through its durable WAL representation. | verified (Phase 1I) |
| STORAGE-97 | Physical WAL deletion remains deferred; logical frontier or obsolescence metadata alone cannot unlink an active WAL or an SSTable without the Phase 1J lifetime proof. | verified (Phase 1I/1J) |
| STORAGE-98 | The fully published sequence never exceeds the last assigned sequence. Readers use the published authority, never the merely assigned authority. | verified (Phase 1J) |
| STORAGE-99 | An admitted batch is installed wholly into the same active MemTable generation to which its contiguous sequence interval was assigned before that interval can be published. | verified (Phase 1J) |
| STORAGE-100 | The visible high-water moves only forward, once per complete batch, after WAL durability and complete MemTable application; a batch has no partially published boundary. | verified (Phase 1J) |
| STORAGE-101 | Every Get and Scan captures one published high-water for its full operation, so no allocated-but-unpublished entry can influence its result. | verified (Phase 1J) |
| STORAGE-102 | A synchronously acknowledged engine batch is recoverable after process loss; a WAL-durable batch whose client result was not delivered is explicitly ambiguous and may recover. | verified (Phase 1J) |
| STORAGE-103 | After restart, CURRENT's selected Manifest and its valid durable prefix exclusively determine the logical Version; physical orphan, temporary and unknown files cannot become live by existence alone. | verified (Phase 1J) |
| STORAGE-104 | WAL reclamation candidates are whole files whose every complete batch ends at or below the already-durable contiguous replay frontier; a straddling batch is invalid. Physical deletion of the active append target is forbidden. | verified candidate computation (Phase 1J) |
| STORAGE-105 | WAL maintenance consumes and never advances the Manifest replay frontier. | verified (Phase 1J) |
| STORAGE-106 | A logically obsolete SSTable is physically deleted only while an exclusive Engine operation-lifetime lock proves that the current Version, retained reads/scans and in-flight compactions cannot reference it. | verified (Phase 1J) |
| STORAGE-107 | Failure or repetition of physical reclamation leaves logical Version authority and latest-state contents unchanged; an extra obsolete file is maintenance debt, not live data. | verified (Phase 1J) |
| STORAGE-108 | Durable file-number authority never decreases or reuses a number across flush, compaction, orphan discovery, reclamation and restart; burned numbers remain burned. | verified (Phase 1J) |
| STORAGE-109 | Durable storage sequences are never reused, and assigned and published authorities are monotonic across flush, compaction and restart. | verified (Phase 1J) |
| STORAGE-110 | A newest tombstone cannot allow an older value to resurrect across flush, compaction, restart or physical obsolete-file reclamation. | verified (Phase 1J) |
| STORAGE-111 | Corrupt authoritative WAL, CURRENT, Manifest or live SSTable state fails explicitly and is never reconstructed heuristically from directory contents; corrupt unlisted SSTables remain non-authoritative. | verified (Phase 1J) |
| STORAGE-112 | Direct MemTable traversal is permitted only after permanent freeze, follows the immutable level-zero order exactly, allocates no full-table snapshot and exposes no mutable MemTable-owned bytes. | verified (Phase 1K) |
| STORAGE-113 | A cached SSTable reader is keyed by one never-reused file identity and usable only under a lease; it cannot be evicted or physically reclaimed while borrowed, retained cache capacity is bounded, and Engine Close drains it under the operation-lifetime lock. | verified (Phase 1K) |
| STORAGE-114 | Performance optimizations do not change the logical Get/Scan result, published sequence boundary, persistent formats, eager integrity boundary or durability acknowledgement point. | verified (Phase 1K) |

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

STORAGE-23 through STORAGE-30 are enforced by
[`memtable_test.go`](../internal/storage/memtable/memtable_test.go), including
binary and prefix counterexamples, exact replacement versus distinct versions,
reference-model lower bounds, stable iterator/range snapshots, caller-buffer
mutation, deterministic accounting, full structural validation, 100,000-entry
stress, 128 concurrent writers/readers and insert-versus-freeze races under the
race detector.

STORAGE-31 through STORAGE-39 are enforced by the writer and test-scoped
structural validation in
[`writer_test.go`](../internal/storage/sstable/writer_test.go) and
[`validate_test.go`](../internal/storage/sstable/validate_test.go). The suite
covers authoritative-order rejection, exact duplicates, binary/prefix/version
round trips, restart and block boundaries, full index-to-data mapping,
metadata/footer integrity, deterministic hashes, every-offset truncation,
protected corruption, bounded malformed fields, I/O poisoning and the
file-sync/rename/directory-sync publication sequence.

STORAGE-4 and STORAGE-40 through STORAGE-47 are enforced by
[`reader_test.go`](../internal/storage/sstable/reader_test.go) through the same
production decoding paths used by Open, Seek and iteration. The suite covers
complete streaming open validation, exact index-to-data boundaries, comparator
lower-bound and version-candidate reference models, multi-block version groups,
full/range iteration, hostile handles and varints, rechecksummed restart
corruption, every-byte and seeded random mutations, every-offset truncation,
injected I/O/short-read/close failures, caller ownership, concurrent reads and
Close races. The opt-in 100,000-entry gate exercises roughly 1,700 blocks;
the exact count varies with its replayable random value stream.

STORAGE-8 is enforced by [`bloom_test.go`](../internal/storage/sstable/bloom_test.go):
10,000 inserted user keys have zero false negatives, a disjoint set measures
false positives, a protected-bit mutation fails CRC, and a rechecksummed filter
with cleared bits fails eager Open rather than skipping stored data. A v1 table
with the optional filter absent remains readable and conservatively returns
"may contain."

STORAGE-48 through STORAGE-59 are enforced by
[`pipeline_test.go`](../internal/storage/pipeline/pipeline_test.go) and the
atomic batch cases in
[`memtable_test.go`](../internal/storage/memtable/memtable_test.go). The suite
checks WAL-before-apply ordering, failure-before-WAL non-effects, batches that
cross the rotation target, explicit lifecycle transitions, stable FIFO retry
identity, bounded cancellable backpressure, post-rename ambiguity, concurrent
accepted-write accounting, graceful shutdown, retained-WAL replay and exact
Phase 1E validation of real Phase 1D flush outputs.

STORAGE-60 through STORAGE-72 are enforced by
[`manifest_test.go`](../internal/storage/manifest/manifest_test.go), the Phase
1G installation states in `pipeline_test.go`, and the production WAL/SSTable
validators. The suite covers deterministic bounded VersionEdit round trips,
atomic invalid edits, duplicate behavior, 10,000-edit model replay,
every-offset Manifest truncation, checksum and semantic corruption, every
CURRENT publication failure boundary including directory fsync, immutable
concurrent reads and serialized installs, orphan/temp collision recovery,
missing/corrupt/mismatched live tables, conservative Manifest/WAL/live sequence
maxima, contiguous-frontier gaps and restart, both sides of the durable AddFile
crash boundary, snapshot rewrite and retained WAL.

STORAGE-73 through STORAGE-84 are enforced by
[`compaction_test.go`](../internal/storage/compaction/compaction_test.go) and
the Phase 1G replacement crash test. Coverage includes transitive L0/L1
user-range closure, deterministic picking, heap ordering, exact multiset
preservation, all versions/tombstones, duplicate-safe failure, multi-output
user-key boundaries, stale plans after durable output, atomic pre/post-Manifest
restart behavior, retained obsolete files and repeated 100-table compaction.

STORAGE-5 through STORAGE-7 and STORAGE-85 through STORAGE-97 are enforced by
[`engine_test.go`](../internal/storage/engine/engine_test.go) together with the
lower-layer crash and failure matrices. Coverage includes binary/prefix keys,
empty values versus tombstones, overlapping L0 and compacted L1 candidates,
full range collapse, read-your-writes, flush/compaction handoff, WAL replay
without reappend, sequence continuation, valid orphan and stale-temp exclusion,
missing/corrupt live-table rejection, fixed/fresh seeded reference models,
restart cycles and concurrent writers/readers/scans/flush/compaction under the
race detector.

STORAGE-98 through STORAGE-111 are enforced by the explicit pipeline
assignment/publication stages, the Engine visibility and reclamation tests,
the subprocess crash test, and the lower-layer crash/corruption matrices. The
suite pauses one three-entry batch at every visibility stage and proves Get and
Scan switch only at the complete publication boundary; distinguishes
non-durable and WAL-durable ambiguous process exits; blocks physical obsolete
SSTable deletion behind a Scan retaining the old Version; injects unlink
failure and retries idempotently; rejects frontier-straddling WAL batches while
leaving active-WAL deletion disabled; and verifies tombstone, orphan and
authority behavior across restart. The opt-in Phase 1J campaign executes
50,000 modeled operations and 120 restarts with full-state/digest checkpoints,
monotonic sequence/file authorities and no identity reuse.

STORAGE-112 through STORAGE-114 are enforced by the frozen-iterator equality
and ownership test, bounded cache reuse/Close tests, the deterministic
borrowed-obsolete-reader reclamation test, and the complete pre-existing
Engine reference, visibility, crash and race suites. The Phase 1K evidence
records the profiles and A/B measurements; performance numbers themselves are
not safety assertions.

## Raft

| ID | Invariant | Status |
|---|---|---|
| RAFT-1 | *Election safety.* At most one leader is elected per term. | verified (Phase 2) |
| RAFT-2 | *Leader append-only.* A leader never overwrites or deletes entries in its own log; it only appends. | verified (Phase 2) |
| RAFT-3 | *Log matching.* If two logs contain an entry with the same index and term, the logs are identical in every entry through that index. | verified (Phase 2) |
| RAFT-4 | *Leader completeness.* If an entry is committed in a term, it is present in the log of every leader of every later term. | verified (Phase 2) |
| RAFT-5 | *State machine safety.* If two nodes apply an entry at a given log index, it is the same entry. No node ever applies conflicting commands at the same index. | verified (Phase 2) |
| RAFT-6 | `currentTerm` never decreases at a node, across restarts included. | verified (Phase 2) |
| RAFT-7 | Within one node execution, `commitIndex` never decreases; `appliedIndex ≤ commitIndex` always. After restart, volatile commit knowledge is reconstructed from the durable snapshot and leader communication. | verified (Phase 2) |
| RAFT-8 | A node grants at most one vote per term, and that grant is durable before the response is sent. | verified (Phase 2) |
| RAFT-9 | A leader that has lost quorum cannot commit new entries, even before it learns it has lost quorum. | verified (Phase 2) |
| RAFT-10 | Persistent state (`currentTerm`, `votedFor`, log entries) is durable before any RPC response depending on it is sent. | verified (Phase 2) |
| RAFT-11 | Log compaction discards only entries covered by a durable snapshot, and a snapshot's applied index is never ahead of what it contains. | verified (Phase 2) |
| RAFT-12 | Installing a snapshot yields a state machine identical to applying every entry the snapshot covers. | verified (Phase 2) |
| RAFT-13 | Entries apply exactly once per node execution, strictly in increasing index order, and only after commitment; an apply failure stops before advancing `lastApplied`. | verified (Phase 2) |
| RAFT-14 | Persistence uncertainty stops a node before it emits any vote, append, snapshot or proposal outcome that depends on the uncertain state. | verified (Phase 2) |

The deterministic simulator checks RAFT-1 through RAFT-8 and RAFT-11/13 at
every scheduled event, including across crashes and restarts. Targeted tests
prove minority and even-cluster quorum behavior (RAFT-9), persistence-before-
response and fatal uncertainty (RAFT-10/14), and snapshot equivalence and
suffix preservation (RAFT-11/12). The complete mapping and campaign record is
in [`evidence/phase-2.md`](evidence/phase-2.md).

## Ranges and routing

### Replicated state-machine integration

The `REPLICA` namespace covers the Phase 3 boundary between one Raft group and
one local LSM. The existing `RANGE` namespace below remains reserved for Phase
4 keyspace ownership and routing.

| ID | Invariant | Status |
|---|---|---|
| REPLICA-1 | A replicated mutation's storage sequence is its Raft log index; no node-local allocator determines replicated order. | verified (Phase 3) |
| REPLICA-2 | Only committed Raft command entries mutate replicated logical LSM state. | verified (Phase 3) |
| REPLICA-3 | Committed mutations apply in strictly increasing Raft-index order; no-op gaps are processed progress, not missing mutations. | verified (Phase 3) |
| REPLICA-4 | Published local applied progress never exceeds complete state-machine application. | verified (Phase 3) |
| REPLICA-5 | The durable applied frontier advances only with contiguous, authoritative flushed-generation coverage. | verified (Phase 3) |
| REPLICA-6 | Durable applied coverage is explicit and is never inferred from SSTable mutation-sequence bounds. | verified (Phase 3) |
| REPLICA-7 | A crash before durable frontier advancement causes committed entries above the old frontier to replay safely. | verified (Phase 3) |
| REPLICA-8 | A crash after durable frontier advancement does not reinsert mutations already represented by authoritative local state. | verified (Phase 3) |
| REPLICA-9 | Flush, compaction and reclamation cannot change replicated command ordering or logical state; compaction cannot advance the applied frontier. | verified (Phase 3) |
| REPLICA-10 | Healthy caught-up replicas at the same applied index have identical logical key/value/tombstone state. | verified (Phase 3) |
| REPLICA-11 | Replica physical LSM layouts may differ; physical identity is never replicated logical authority. | verified (Phase 3) |
| REPLICA-12 | Standalone WAL and local-sequence semantics are isolated from replicated engine mode, which emits no data-WAL write. | verified (Phase 3) |
| REPLICA-13 | Malformed commands or uncertain committed state-machine application stop the replica before it skips or advances past the entry. | verified (Phase 3) |
| REPLICA-14 | Client mutation success requires Raft quorum commitment and leader-local application. | verified (Phase 3) |

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
