# Design Note: Local Storage Engine (Phase 1)

**Status:** Phase 1A storage primitives, Phase 1B WAL, Phase 1C MemTable, Phase
1D SSTable format/writer, Phase 1E production reader and Phase 1F MemTable
rotation/flush pipeline are implemented and mechanically tested. Later engine
layers are not implemented. The rest of this note specifies later Phase 1 work.

**Related:** [ADR-0001](design-decisions/0001-lsm-tree-over-b-tree.md) ·
[ADR-0003](design-decisions/0003-explicit-internal-key-comparator.md) ·
[ADR-0004](design-decisions/0004-wal-integrity-and-tail-recovery.md) ·
[ADR-0005](design-decisions/0005-memtable-skip-list.md) ·
[ADR-0006](design-decisions/0006-sstable-physical-format.md) ·
[ADR-0007](design-decisions/0007-sstable-reader-validation-and-seek.md) ·
[ADR-0008](design-decisions/0008-memtable-rotation-and-flush-lifecycle.md) ·
[invariants.md](invariants.md) (STORAGE-1 … STORAGE-59) ·
[architecture.md](architecture.md) §3

---

## 1. Problem

RivetDB needs a single-node, durable, ordered key-value store. Every layer
above it — MVCC versions, Raft logs, Raft snapshots, range data — ends up as
bytes here.

The engine is the foundation of the whole system's durability guarantee. If it
loses an acknowledged write on a crash, no amount of replication above it
helps: Raft's guarantee is "a committed entry survives on a majority", and it
is stated in terms of what each node durably kept. A storage engine that lies
about durability makes every guarantee above it false.

## 2. Requirements

| # | Requirement | Why |
|---|---|---|
| R1 | Durable writes: acknowledged writes survive process and machine crashes when the durability mode promises it | Raft's persistence obligations rest on this |
| R2 | Ordered iteration over keys | range scans, range splits, and range-based sharding all need it |
| R3 | Point lookup faster than reading every file | the read path is the common case |
| R4 | Bounded, tunable space and write amplification | a range must not consume unbounded disk relative to its live data |
| R5 | Multi-version friendly key encoding | MVCC (Phase 5) stores many versions per user key and reads at a timestamp |
| R6 | Corruption detection on every persisted byte | disks do not fail cleanly, and a silently corrupt replica is worse than a dead one |
| R7 | Recovery from a crash at *any* instruction | this is the property the tests actually exercise |
| R8 | Concurrent readers unblocked by writers, flushes and compactions | a range serves reads while it compacts |
| R9 | Deterministic behaviour under test | flushes and compactions must be triggerable explicitly, not only by background timing |
| R10 | The format is versioned | it will change, and a version byte is much cheaper than a migration guess |

## 3. Non-goals for Phase 1

- **Replication, networking, transactions.** The engine knows nothing about
  clusters. It is a local database.
- **A block cache.** Correctness first; caching is a Phase 11 optimisation with
  a measurement attached.
- **Secondary indexes, merge operators, column families.** No user needs them
  here.
- **Concurrent writers.** Writes are serialised through a single write path.
  Everything above the engine (Raft apply) is already serialised, so a
  multi-writer engine would add lock complexity for no benefit.

## 4. Architecture

```text
                Put/Delete (a batch is atomic)
                          │
                          ▼
            ┌───────────────────────────┐
            │   WAL: append + checksum  │  optional fsync before ack
            └─────────────┬─────────────┘
                          ▼
            ┌───────────────────────────┐
            │  MemTable (skip list)     │  writes visible immediately
            └─────────────┬─────────────┘
                          │ size threshold reached
                          ▼
            ┌───────────────────────────┐
            │ Immutable MemTable        │  still readable; a fresh MemTable
            └─────────────┬─────────────┘  and WAL take new writes
                          │ background flush
                          ▼
            ┌───────────────────────────┐
            │      SSTable in L0        │  files may overlap in key range
            └─────────────┬─────────────┘
                          │ compaction
                          ▼
            ┌───────────────────────────┐
            │    L1 … Ln SSTables       │  non-overlapping within a level
            └───────────────────────────┘

  Version set (immutable): which files exist at which level, right now.
  Manifest (on disk):      the durable log of changes to the version set.
```

**Read path.** MemTable → immutable MemTable → L0 files newest-first → L1…Ln by
binary search within each level. Stop at the first entry for the key: because
newer data is always consulted first, that entry is the correct answer.
A Bloom filter per SSTable skips files that cannot contain the key (STORAGE-8);
in a levelled layout most point lookups touch one data block.

**Write path.** A batch is appended to the WAL, optionally fsynced, then
applied to the MemTable. WAL-before-MemTable is what makes recovery possible:
anything visible to a reader is already durable, so replay can only add, never
subtract.

### 4.1 MemTable

The MemTable is a custom skip list ordered solely by `CompareInternal`. Its
level-zero chain therefore contains every internal entry in exact future
SSTable order: user key ascending, sequence descending, then kind ascending.
It preserves different versions and kinds as different entries. Reinserting
the exact same internal key atomically replaces its value; this makes replay or
an idempotent apply retry converge without creating comparator-equal physical
duplicates.

The list has maximum height 20 and probability 1/4 of promotion at each level.
Production topology uses a normal concurrency-safe randomness source and tests
inject a seeded source. Topology changes performance and layout only. All
searches and structural validation use the §5.2 comparator, never the encoded
representation or a second comparison implementation. Expected complexity is
O(log n) for insert, exact get, lower-bound seek and version-candidate lookup;
a pathological topology is O(n). Forward iterator steps are O(1), iterator
construction is O(n), and table memory is O(n).

An exact `Get` looks for one complete internal key. A version-aware lookup
constructs `(user key, target sequence, deletion)` and returns the comparator
lower bound only if its user key is equal, yielding the newest candidate whose
sequence is at most the target. It returns a value or tombstone without
interpreting visibility beyond that candidate. General `Seek(K)` returns the
first internal entry not less than `K`.

Range bounds are decoded user keys. A half-open `[start,end)` iterator includes
all versions and both kinds for each user key in that interval; nil denotes an
unbounded side. Bounds are never converted to encoded internal keys for the
upper-bound check, so no boundary can split a version group. Full and range
iterators snapshot ordered entries while holding a read lock, then traverse the
private snapshot without locks. An iterator is consequently stable across
later inserts, replacement and freeze, at the cost of reader-local copying.

One `sync.RWMutex` gives the Phase 1C object a linearizable concurrency
contract. Writers serialize; readers, seeks and iterator snapshot construction
may run concurrently. `Freeze` takes the write lock and is permanent and
idempotent. An insert racing with freeze either completes wholly before freeze
or returns `ErrFrozen`; after `Freeze` returns no insert can succeed. Reads and
new iterators remain valid.

Insertion copies the internal key and value. Reads and iterators also return
copies, so caller mutation cannot change ordering or stored data. A tombstone
is distinguished by its key kind and accepts no value; an empty PUT remains a
value entry with a zero-length value.

`SizeBytes` is a deterministic approximation rather than Go heap telemetry. It
includes table and head-node overhead, node metadata, owned key bytes, retained
value-buffer capacity and forward-link slots. Exact-key replacement reuses or
grows the retained buffer, so the estimate is monotonic while mutable and
stable after freeze. Saturating addition prevents wraparound. Reader-owned
iterator snapshots are excluded. `ReachedSize(target)` reports whether a later
rotation policy should act, but Phase 1C performs no automatic rotation or
flush.

### 4.2 Phase 1F write and flush pipeline

`internal/storage/pipeline` owns one `SyncBatch` WAL writer, one active mutable
MemTable and one bounded FIFO of frozen generations. One serialized admission
token orders concurrent batches and assigns their contiguous sequence ranges.
The order is validate and assign, encode, WAL append/fsync, atomic MemTable
`ApplyBatch`, acknowledgement, then a size check. A batch reaching or crossing
the configured `SizeBytes` target rotates only after the whole batch is
applied, so no batch spans generations and one batch may exceed the target.

Rotation freezes the old generation, assigns it a stable in-memory SSTable
file number, installs exactly one successor and queues the old generation under
the coordinator mutex. Generation and file allocators are deliberately not
restart-safe before Phase 1G. One background worker claims only the FIFO head
and moves it through `queued → flushing → durable` or `failed`. It streams the
frozen iterator through the Phase 1D writer and does not remove the generation
until Phase 1E reopens the published file and proves exact ordered equality.

When the immutable count reaches its configured bound, the next writer waits
on a bounded notification channel before WAL append. Cancellation during that
wait returns without a durable effect; after append begins, cancellation cannot
misreport an accepted write. A failed FIFO head is retained and blocks later
flushes. Explicit retry preserves generation/file identity and is permitted
only when publication is not ambiguous. A final `.sst` observed after failure
is never automatically removed or overwritten; `.tmp` reconciliation is a
future manifest/startup-cleanup concern.

Phase 1F uses a flat directory: `000000000001.wal`, `%012d.sst.tmp` and
`%012d.sst`. It never creates a manifest and never truncates or deletes the
WAL. Clean shutdown first excludes in-flight/new writes, drains generations
already queued, joins the worker, closes the WAL and leaves a nonempty active
MemTable WAL-backed for replay. Replay intentionally reapplies all retained
history through `MemTable.ApplyBatch`; deciding which flushed history may be
skipped requires Phase 1G manifest authority.

## 5. On-disk format

Fixed-width integers are **little-endian**, except where big-endian is required
so that lexicographic byte order matches numeric order. Variable-length
integers are shortest-form unsigned LEB128; non-canonical alternatives are
rejected. Checksums are **CRC32C** (Castagnoli), chosen for hardware
acceleration on both amd64 and arm64.

Files in the data directory:

| Name | Contents |
|---|---|
| `LOCK` | an advisory lock file; a second process must not open the same directory |
| `CURRENT` | one line naming the active manifest file |
| `MANIFEST-%06d` | the version-edit log |
| `%06d.log` | a WAL segment |
| `%012d.sst` | an SSTable; the same name plus `.tmp` is unpublished build state |

### 5.1 WAL

A WAL segment is a sequence of 32 KiB blocks. Logical records are framed within
blocks and fragmented across them when needed:

```text
 0           4          8        10     11                    11+len
 ┌───────────┬──────────┬────────┬──────┬──────────────────────┐
 │content CRC│header CRC│ length │ type │       payload        │
 │    u32    │   u32    │  u16   │  u8  │      len bytes       │
 └───────────┴──────────┴────────┴──────┴──────────────────────┘

 type low nibble:  1 FULL    the whole record is in this fragment
                   2 FIRST   the first fragment of a record
                   3 MIDDLE  a middle fragment
                   4 LAST    the final fragment
 type high nibble: format version, 0 for this format
```

Both checksums are CRC32C. Header CRC covers the two encoded length bytes and
the type byte. It is checked before length or type is trusted. Content CRC
covers those same three bytes followed by the fragment payload. Thus corruption
of either checksum, length, type/version or payload is detected. A block's
trailing bytes are zero-padded when fewer than 11 remain; a reader accepts only
zero padding there.

The independent header checksum is load-bearing. Without it, a corrupted
length that extends beyond EOF is indistinguishable from a crash-truncated
payload because the content checksum cannot be computed. Fixed blocks also
bound every physical fragment and keep malformed lengths from moving parsing
outside the current block. A corrupt fragment is never skipped or
resynchronised past during recovery: the valid-prefix proof ends there.

The maximum logical record is 64 MiB. Physical fragment length is additionally
bounded by its containing block. Readers validate both limits before growing a
buffer, so persisted length fields cannot cause overflow or unbounded
allocation. Zero-length logical records are valid at the framing layer.

The payload of a FULL/FIRST fragment is a write batch:

```text
 ┌───────────────┬───────────┬──────────────────────────────┐
 │ first seq u64 │ count u32 │  count × (kind, key, value)  │
 └───────────────┴───────────┴──────────────────────────────┘

 entry:  kind u8   (0 = delete, 1 = value)
         key       varint length + bytes
         value     varint length + bytes (absent when kind = delete)
```

The fixed fields are little-endian. A batch contains at least one entry. The
caller supplies `first sequence`; entry `i` has sequence `first+i`, and a batch
whose range would overflow `uint64` is rejected. Empty user keys and empty PUT
values are valid. DELETE has no value-length field, so it is distinct from PUT
of an empty value. A batch is the unit of atomicity: decoding produces all
entries or an error, and replay applies all of it or none of it.

**Recovery and offsets.** Every returned logical record includes the physical
offset of its first fragment header and the exclusive end offset of its last
fragment. Recovery returns records in that order and reports `valid end`, the
exclusive file offset through the last complete logical record. It streams
records to a callback and does not load the whole segment.

Exact EOF after a complete record is clean. EOF in a header, authenticated
payload, block padding, or fragmented logical record is a recoverable truncated
tail: the incomplete logical record is not returned and `valid end` points to
its first fragment (or the preceding record for incomplete padding). A checksum
failure, invalid type/version, nonzero padding, impossible fragment sequence or
authenticated out-of-block length is corruption at any position, including
the file tail. Recovery stops and returns the error; it never skips the region
or destroys evidence.

Reading never mutates a WAL. Tail repair is explicit and accepts a recovery
result only after rescanning the unchanged file. It truncates to `valid end`
and fsyncs the file. Corrupt WALs are never repairable through this operation.
After repair, a writer may append without resurrecting discarded fragment
bytes. Repair requires exclusive ownership of the database directory and no
open WAL writer; Phase 1's `LOCK` file supplies that cross-process exclusion
when the engine is assembled.

**Writer and durability contract.** A writer serialises concurrent `Append`
calls. It handles short writes until the fragment is complete. Any write or
sync failure poisons the writer so later appends cannot pretend the stream is
healthy; `Close` remains available and is idempotent. Append and Sync after
Close return a sentinel error.

`SyncBatch` is the default: `Append` acknowledges only after all fragments and
padding have been written and the WAL file's `fsync` succeeds. For a newly
created segment, its first durability boundary then fsyncs the containing
directory before acknowledging, so the filename itself survives a crash.
`SyncNone` acknowledges after the complete logical record has reached the OS
through successful writes; a later explicit `Sync` establishes a durability
boundary for all prior successful appends. `SyncInterval` remains deferred
because it requires asynchronous group-commit policy not needed in Phase 1B.

The contract assumes successful writes update the file in order, `fsync`
persists preceding file data and size metadata across process/machine restart,
and the filesystem/device honors that success. `Close` alone is not a
durability boundary. Hardware that loses or silently rewrites successfully
synced data is outside this guarantee; checksums detect corruption except for
the inherent possibility of a CRC collision.

### 5.2 Internal keys

Every key stored in a MemTable or SSTable carries a trailer:

```text
 internal_key = user_key ‖ BE64(^sequence) ‖ kind_u8

   sequence  monotonically increasing, unique per write
   ^         bitwise complement
   kind      0 = deletion (tombstone), 1 = value
```

This encoding is self-parsing because the trailer is fixed-width, but it is
**not** order-preserving under a raw `bytes.Compare`. For example, with
sequence 0 and deletion kind, the encoding of user key `"a"` begins
`61 ff`, while the encoding of user key `"aa"` begins `61 61`. Raw comparison
therefore reverses the user-key order. The trailer of a shorter key must never
be compared with a byte remaining in a longer, prefix-related user key.

All sorted components use one explicit internal-key comparator:

1. compare user keys with `bytes.Compare`;
2. if equal, compare sequence numbers descending (higher/newer first);
3. if still equal, compare kind ascending (`deletion` before `value`).

The third step makes the comparator a strict total order even though sequence
numbers are unique per write and a valid engine state ordinarily cannot have
both kinds at one `(user key, sequence)`. The complemented big-endian sequence
stays in the encoded representation: it gives the desired byte order within an
already-established equal-user-key group and keeps the format compact and easy
to inspect. It does not replace the comparator. These rules are invariants
STORAGE-12 through STORAGE-15 and are mechanically enforced before Phase 1.

This is also the hook for MVCC (R5). In Phase 5 commit timestamps map onto the
sequence space. A snapshot lookup at `T` constructs logical seek key
`(user key, T, deletion)` and takes the comparator lower bound; using the
minimum kind ensures a value or tombstone exactly at `T` is not skipped. All
versions of one user key are contiguous, so iteration and skipping older
versions compare the decoded user-key field. No file-format change is needed.

A tombstone is an ordinary entry with `kind = 0` and no value. It shadows older
versions until compaction can prove they are unreachable.

**Range boundaries are user keys.** A range `[a, m)` contains every internal
version whose user key is in that interval. Split selection, routing, scan
bounds and shared-SSTable filtering never compare a range boundary with an
encoded internal key. A split therefore cannot divide the versions of one
logical key (STORAGE-16).

**SSTable comparator contract.** Data blocks and sparse indexes are ordered by
the explicit comparator. Phase 1 initially stores each data block's full last
internal key in the index. A seek binary-searches for the first index key not
less than its logical internal seek key, then uses the same comparator within
that block. Short separators are optional; if later added, they must be
constructed at the user-key layer and proved to preserve these lower-bound
semantics. Lower range bounds use `(user key, max sequence, deletion)`; upper
bounds are enforced by comparing decoded user keys, which also handles a bound
with no finite bytewise successor such as an all-`ff` key.

### 5.3 SSTable

```text
 ┌────────────────────────────────┐
 │  data block 0                  │  target 4 KiB payload
 │  data block 1                  │
 │  …                             │
 ├────────────────────────────────┤
 │  index block                   │  full last key + data handle
 ├────────────────────────────────┤
 │  metadata block                │  counts and key bounds
 ├────────────────────────────────┤
 │  filter block (future/absent)  │  user-key Bloom payload
 ├────────────────────────────────┤
 │  footer (fixed 80 bytes)       │  discoverable from EOF
 └────────────────────────────────┘
```

Format version 1 uses explicit byte encoding, never Go layout. Fixed-width
integers are little-endian except the internal key's complemented sequence
trailer (§5.2). Variable lengths are canonical unsigned LEB128.

**Common block envelope.** Every independently readable block is:

```text
 payload || compression_u8 || kind_u8 || block_version_u8 || crc32c_u32

 compression: 0 = none
 kind:        1 = data, 2 = index, 3 = metadata, 4 = reserved filter
 block version: 1
 crc32c: little-endian CRC32C(payload || compression || kind || version)
```

The seven-byte envelope is included in every block handle length. Unknown
compression, kind or block version is unsupported/corrupt input, never guessed.

**Data block.** Encoded internal keys are prefix-compressed against the
previous encoded key. This reconstructs bytes only; ordering is validated with
`CompareInternal`. Every 16th entry is a restart and stores a full key:

```text
 entry:   shared    varint  bytes shared with the previous key
          unshared  varint  bytes that follow
          vallen    varint
          keydelta  unshared bytes
          value     vallen bytes

 payload trailer:
          restarts  u32 LE × n   entry offsets from payload start
          n         u32 LE
```

The first entry of every nonempty block is a restart with `shared=0`; restart
offsets are strictly increasing. A block never splits an entry. The default
target is 4096 payload bytes including the restart array/count but excluding
the common envelope. Before an entry is added, the writer cuts a nonempty block
if its projected payload would exceed the target. A legal larger entry becomes
one oversized block. The restart count and offsets are u32-bounded.

Resource limits in version 1 are: user key 1 MiB, internal key 1 MiB + 9-byte
trailer, value 64 MiB, encoded entry `internal + value + 30` bytes, and table
size `MaxInt64`. Checked arithmetic rejects any overflow before allocation or
write. Configured block targets cannot exceed the maximum legal data payload.

**Index block.** Its payload begins with `count_u32`, followed by:

```text
 key_len varint || full_last_internal_key || offset_u64 || length_u64
```

There is one entry per data block. `offset` starts at the file beginning and
`length` includes the block envelope. Index keys are the exact full last keys,
not shortened separators. Future seek chooses the first index key not less
than its internal seek key using §5.2, then searches that block. Multiple
versions may cross a physical boundary without changing this rule.

**Metadata block.** Its payload is:

```text
 entry_count_u64 || deletion_count_u64 || data_block_count_u32 ||
 raw_key_value_bytes_u64 ||
 smallest_internal_len_u32 || smallest_internal ||
 largest_internal_len_u32  || largest_internal  ||
 smallest_user_len_u32     || smallest_user     ||
 largest_user_len_u32      || largest_user
```

Internal bounds are the first and last entries under the authoritative order.
User bounds are their decoded user keys. No owner range ID is stored, so future
child ranges may share a physical immutable file and apply `[start,end)` user
bounds externally.

**Filter block.** Deferred to a later phase. Version 1 reserves block kind 4,
a footer handle and flag. Phase 1D writes `(0,0)` with the flag clear. A future
Bloom payload hashes decoded user keys only and can be inserted before the
footer without changing its layout. No filter query logic exists in Phase 1D.

**Footer.** Exactly 80 bytes at EOF:

```text
  0  index_offset_u64       8  index_length_u64
 16  metadata_offset_u64   24  metadata_length_u64
 32  filter_offset_u64     40  filter_length_u64
 48  data_region_end_u64
 56  format_version_u32 (=1)
 60  flags_u32          (bit 0 = filter present; currently 0)
 64  reserved_u32       (=0)
 68  footer_crc32c_u32
 72  magic_u64          (=0x5453535445564952; LE bytes are ASCII "RIVETSST")
```

The footer checksum covers bytes `[0,68)` and `[72,80)`, authenticating every
other footer field. Handles are u64 offset/length pairs and must add without
overflow, lie before the footer, name non-overlapping correctly typed blocks,
and account for every byte: contiguous data region, then index, then metadata,
then optional filter, then footer. No trailing garbage is accepted. An unknown
major version, bad magic/checksum, invalid handle, malformed block or truncated
file is corruption. SSTables have no recoverable-tail semantics.

**Writer and publication.** Input must be strictly increasing under
`CompareInternal`; exact duplicates and descending input are errors, never
silently sorted. `Finish` rejects an empty table, flushes the final data block,
writes index/metadata/footer, fsyncs and closes the deterministic
`%012d.sst.tmp`, renames it to `%012d.sst`, then fsyncs the directory. Only then
does it return success and metadata. Repeated successful `Finish` returns the
same metadata; `Add` after finish fails. Any I/O failure poisons the writer.
`Abort` explicitly closes and removes only the temporary path when safe;
ambiguous post-rename evidence is retained. Manifest installation remains
deferred, so the published file is durable but not yet live in a version set.
Creation through publication requires exclusive database-directory ownership;
Phase 1's `LOCK` prevents a competing creator from replacing the same identity.

**Production reader.** Phase 1E uses `os.File.ReadAt`, loads copied index and
metadata state, and deliberately streams every data block during `Open`. This
full validation is required before the sparse index is trusted: an ordered,
rechecksummed but false last-key boundary could otherwise route Seek past the
correct block. Open retains no data blocks and never reads the whole file into
memory at once. Each later access rereads one block, verifies its envelope and
CRC before semantic decoding, validates canonical lengths and restart points,
then reconstructs keys under `CompareInternal`.

Seek finds the first full index boundary not less than its target, binary
searches independently decodable restart keys, and linearly decodes within one
restart interval. After the mandatory O(block bytes) checksum pass, its search
and decoding cost is O(log B + log R + I), with B data blocks, R restarts in
the selected block and restart interval I. Exact lookup is Seek
plus comparator equality. Candidate lookup constructs `(user key, target
sequence, deletion)` once and preserves tombstones. Iterators retain one
decoded block, cross blocks in physical order, and return copied key/value
entries. User ranges are decoded half-open `[start,end)` bounds; nil is
unbounded and a reversed range is rejected.

Readers support concurrent read-only methods and independent iterators without
a shared block cache. Close is idempotent and excludes active reads. A Reader
must outlive its iterators; all operations fail with `ErrClosed` after it is
closed. Bloom absence is understood but no Bloom lookup or construction exists.
See [ADR-0007](design-decisions/0007-sstable-reader-validation-and-seek.md).

**Ordering within a level.** L0 files may overlap, because they are flushed
MemTables and each covers whatever keys happened to be in memory; they are
searched newest-first. L1 and below are non-overlapping, so a lookup binary
searches the level and reads at most one file.

### 5.4 Manifest and version set

Phase 1G implements this section according to
[ADR-0009](design-decisions/0009-manifest-versionset-and-replay-frontier-authority.md).
Physical SSTable presence is never authority: CURRENT selects one Manifest,
Manifest replay constructs one immutable Version, and only its listed tables
are live. Directory scanning classifies orphans/temporaries and raises the
allocation floor, but never adds a table.

A *version* is the immutable set of files at each level. The manifest is an
append-only log — reusing the WAL's block framing (§5.1) — of *version edits*:

```text
 VersionEdit {
     comparator name       (once, in the first record)
     log number            WAL segments before this are obsolete
     next file number
     last sequence
     deleted files         (level, file number) …
     added files           (level, file number, size, smallest key, largest key) …
 }
```

Recovery reads `CURRENT`, opens the named manifest, and replays every edit to
rebuild the current version. The set of files that exist and the set the
manifest describes is authoritative: missing or mismatched live files are
corruption, while unlisted physical files are reported as orphans and retained.

The Phase 1G edit payload is deterministic binary TLV inside the WAL framing.
It persists comparator identity, AddFile/DeleteFile operations, monotonic next
file and last-sequence fields, and an optional inclusive replay frontier. Each
added table includes Phase 1D metadata plus its flush generation and contiguous
sequence span. Candidate Version construction and semantic validation precede
Manifest append/fsync; the immutable current pointer changes only afterward.
L0 is deterministically newest-first and may overlap. Higher levels are
represented and require non-overlap when populated, but Phase 1G executes no
compaction.

The VersionEdit v1 payload is byte-exact:

```text
offset  size  field
0       4     magic 0x44455652 little-endian (bytes "RVED")
4       2     version = 1, little-endian
6       2     field count, little-endian
8       ...   repeated: tag u8, payload length u32 LE, payload bytes
```

Known tags are comparator `1` (bytes treated as an opaque identity,
maximum 128 bytes), next file `2`, last sequence `3`, replay frontier `4`
(each an LE64), DeleteFile `5` (`level LE32, file LE64`) and AddFile `6`.
Tags with the high bit clear are required and unknown values reject the edit;
unknown high-bit tags are optional and skipped by their bounded length.

An AddFile payload is, in order: `level LE32`; six LE64 values for file number,
flush generation, file size, entry count, deletion count and raw key/value
bytes; data-block count LE32; smallest and largest sequence LE64; then four
`length LE32 + bytes` values for smallest internal key, largest internal key,
smallest user key and largest user key. Internal keys retain §5.2's explicit
encoding/comparator contract. An edit is at most 16 MiB, contains at most 4,096
adds and 4,096 deletes, and uses levels 0 through 7. Scalar tags occur at most
once. Encoders sort deletes and adds by `(level,file)` so equivalent edits have
identical bytes.

The replay frontier means every complete batch ending at or below it is already
represented by authoritative installed state. It advances only through
gap-free installed flush spans; later installed spans beyond a gap do not move
it. A batch crossing the boundary is an error rather than a partial replay.
The frontier is durable metadata only: Phase 1G does not delete WAL files.

`CURRENT` is replaced by writing a temporary file, fsyncing it, renaming it over
`CURRENT`, and then **fsyncing the containing directory**. The last step is the
one that is usually forgotten: without it, a rename can be lost across a crash
even though the file's contents were durable, and the database opens against a
stale manifest.

The same discipline applies to every new file: contents fsynced, then the
directory fsynced, and only then is the file referenced from the manifest
(STORAGE-11). A reader therefore never sees a file that is not fully durable.

## 6. Durability modes

| Mode | Behaviour | Loses on machine crash | Intended use |
|---|---|---|---|
| `SyncNone` | write to the OS page cache, no fsync | recent writes | bulk load, benchmarks |
| `SyncBatch` | fsync before acknowledging each batch | nothing | Raft log, default |
| `SyncInterval(d)` | deferred; future asynchronous group commit | up to `d` of writes | throughput-oriented |

`SyncBatch` is the default because the engine's main consumer is a Raft log,
where "acknowledged" must mean "durable" or RAFT-10 is violated. `SyncNone`
exists so benchmarks and bulk loading can quantify or accept the fsync tradeoff;
the mode in use is always reported alongside a benchmark number.

Process-crash versus machine-crash is distinguished explicitly: a write in the
page cache survives a process crash but not a power loss, and a document that
conflates the two is describing a guarantee it does not have.

## 7. Failure scenarios

| Scenario | Handling | Invariant |
|---|---|---|
| Crash mid-WAL-append | a structurally incomplete trailing record is reported as a repairable tail; earlier complete records replay | STORAGE-3 |
| Crash after WAL fsync, before MemTable apply | replay re-applies the batch | STORAGE-1 |
| Crash during flush | the partial SSTable is never in the manifest, so it is ignored and deleted; the WAL still holds the data | STORAGE-9, STORAGE-11 |
| Crash during compaction | inputs remain live because the version edit was never applied; the partial output is orphaned and removed | STORAGE-7 |
| Crash between file fsync and manifest update | the file is orphaned; recovery deletes files absent from the manifest | STORAGE-9 |
| Crash between manifest write and `CURRENT` rename | the old manifest is still valid; the new one is orphaned | STORAGE-9 |
| Bit flip in a data block | the block CRC fails; the read returns an error rather than wrong data | STORAGE-2 |
| Bit flip in the index or footer | detected on open; the file is rejected | STORAGE-2 |
| Truncated SSTable | the footer magic or the file length check fails on open | STORAGE-2 |
| Disk full during flush | the flush fails and is retried; the MemTable stays in memory and the WAL keeps the data; writes are throttled | — |
| Two processes open one directory | the second fails on `LOCK` | — |

The Phase 1 gate requires a test that crashes the engine at *every* write offset
in a WAL segment and asserts that recovery yields a valid prefix at each one.
Testing a handful of hand-picked crash points is not evidence.

## 8. Concurrency model

Ownership, stated explicitly:

| State | Owner | Access by others |
|---|---|---|
| WAL writer, active MemTable | the single write goroutine | none |
| Immutable MemTable | created by the writer, read by everyone | read-only after sealing |
| Version set | the version-set mutex | readers take a reference-counted snapshot |
| SSTable files | immutable once written | concurrent readers, no lock |
| Compaction state | the compaction goroutine | reports results through the version set |

The design principle is that **the mutable set is small and the immutable set is
large**. Only two things mutate: the active MemTable and the current version
pointer. Everything else — sealed MemTables, SSTables, versions — is immutable
once published, so readers need no locks and are never blocked by a flush or a
compaction (R8, STORAGE-10).

A reader takes a reference to the current version and holds it for the duration
of its iteration. Compaction may publish a new version and mark old files
obsolete, but a file is not deleted until its reference count reaches zero. That
is what lets a long scan run correctly while compaction rewrites the levels
underneath it.

Backpressure: if flushes cannot keep up, writes are throttled and then blocked,
rather than allowing memory to grow without bound. An engine that accepts writes
faster than it can persist them converts a slow disk into an out-of-memory
crash.

For determinism (R9), background work is triggerable explicitly. Tests call
`FlushNow` and `CompactNow` and wait for completion, instead of sleeping and
hoping.

## 9. Alternatives considered

**B+ tree instead of an LSM tree.** See
[ADR-0001](design-decisions/0001-lsm-tree-over-b-tree.md). Summary: RivetDB's
write path is a Raft apply loop — an ordered, write-heavy stream — and range
splitting benefits from immutable files that can be referenced by two ranges
without copying. Both favour an LSM tree. The cost is read amplification, which
Bloom filters and levelling bound.

**Size-tiered compaction instead of levelled.** Size-tiered writes less
(roughly O(log n) rewrites per byte versus O(level) for levelled) but leaves
many overlapping files per level, so a point lookup may consult all of them and
space amplification can reach 2×. RivetDB chooses **levelled**: read
predictability matters more here, because a hot range's read latency is the
signal the rebalancer acts on, and a compaction strategy that makes P99 latency
depend on how recently a level was merged makes that signal noisy.

**mmap instead of pread.** mmap is simpler and avoids a copy, but an I/O error
arrives as `SIGBUS` rather than as an error value, which is unacceptable for
R6 — corruption must be a handled error, not a crash. pread also gives explicit
control over what is resident. Choosing pread.

**Per-block checksums instead of per-record.** Per-record checksums in the WAL
allow the exact torn record to be identified and the rest to be recovered.
Per-block would be marginally cheaper and much coarser. Choosing per-record for
the WAL and per-block for SSTables, since an SSTable block is the unit of
reading anyway.

**A separate write-ahead log per column family or per range.** Not applicable in
Phase 1 (one engine, one log), but noted because Phase 4 gives each node many
ranges. Whether ranges share one engine and one WAL, or each gets its own, is a
Phase 4 decision with real consequences for fsync amortisation — one shared log
turns N per-range fsyncs into one. Flagged now so the Phase 1 interfaces do not
foreclose it.

## 10. Tradeoffs accepted

- **Read amplification.** A point lookup may consult several levels. Bloom
  filters make the common case one block read; range scans genuinely must merge
  across levels.
- **Write amplification.** Levelled compaction rewrites each byte roughly once
  per level. This is measured and published in the Phase 1 benchmark rather
  than estimated.
- **Space amplification.** Obsolete versions persist until compaction reclaims
  them. Bounded by the level size ratio, and measured.
- **Compaction latency spikes.** Background compaction competes with foreground
  I/O. Rate limiting is a Phase 11 concern; Phase 1 measures the effect first.
- **Single writer.** Caps single-engine write throughput. Acceptable because
  the layer above is already serialised, and because a node's parallelism comes
  from hosting many ranges.

## 11. Tests

The Phase 1 gate. Every item is a test that must exist and pass.

**Unit**
- Internal key encode/decode round-trip; ordering property verified over
  randomized key and sequence pairs against a reference comparator.
- WAL record framing: fragmentation across block boundaries, padding, strict
  stop at corruption, and bounded decoding.
- Block builder and reader: prefix compression, restart points, binary search
  within a block.
- Bloom filter: no false negatives over a large randomized key set; measured
  false-positive rate within tolerance of the theoretical value.
- Footer and index encode/decode, including rejection of a bad magic and of an
  unknown version.

**Property / model-based**
- Randomized operation sequences (`Put`, `Delete`, `Get`, `Scan`) against a
  reference `map` plus a sorted key list, comparing after every operation, with
  flushes and compactions interleaved at random points. Seeded via
  [`testutil.Seed`](../internal/testutil/seed.go); failing seeds committed.

**Recovery**
- Crash at every write offset in a WAL segment; recovery yields a valid prefix
  each time (STORAGE-1, STORAGE-3).
- Crash during flush, during compaction, between file creation and manifest
  update, and between manifest write and `CURRENT` rename (STORAGE-9,
  STORAGE-11).
- Recovery is idempotent: opening, closing and reopening repeatedly converges.

**Corruption**
- A flipped bit in every structural position — WAL record, data block, filter
  block, index block, footer — is detected (STORAGE-2).
- Truncation at arbitrary offsets in an SSTable is detected on open.

**Compaction**
- A deleted key stays deleted across compaction (STORAGE-6).
- Compaction preserves the value visible at every key and sequence (STORAGE-7).
- Post-compaction structural check under `invariant.Expensive()`: keys ordered
  within each file, levels non-overlapping below L0 (STORAGE-4).

**Concurrency**
- Iterators opened before a flush or compaction are unaffected (STORAGE-10).
- Concurrent readers with a writer under `-race`.
- `testutil.NoLeaks` on open/close cycles: no goroutine survives `Close`.

**Benchmarks (baseline, recorded not asserted)**
- Sequential and random `Put` throughput per durability mode.
- Point `Get` hit and miss; `Scan` throughput.
- Write amplification, space amplification, compaction time share.
- Recovery time as a function of WAL size.
