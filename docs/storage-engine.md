# Design Note: Local Storage Engine (Phase 1)

**Status:** Phase 1A storage primitives and Phase 1B WAL implemented; the
MemTable and later engine layers are not implemented. The internal-key contract
in §5.2 and WAL contract in §5.1 have mechanical tests. The rest of this note
specifies later Phase 1 work.

**Related:** [ADR-0001](design-decisions/0001-lsm-tree-over-b-tree.md) ·
[ADR-0003](design-decisions/0003-explicit-internal-key-comparator.md) ·
[ADR-0004](design-decisions/0004-wal-integrity-and-tail-recovery.md) ·
[invariants.md](invariants.md) (STORAGE-1 … STORAGE-22) ·
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
| `%06d.sst` | an SSTable |

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
 │  data block 0                  │  ~4 KiB, sorted internal keys
 │  data block 1                  │
 │  …                             │
 ├────────────────────────────────┤
 │  filter block                  │  Bloom filter over user keys
 ├────────────────────────────────┤
 │  index block                   │  one entry per data block
 ├────────────────────────────────┤
 │  footer (fixed 48 bytes)       │
 └────────────────────────────────┘
```

**Data block.** Keys are prefix-compressed against the previous key, with a
full key written every 16 entries (a *restart point*) so that a binary search
within the block is possible:

```text
 entry:   shared    varint  bytes shared with the previous key
          unshared  varint  bytes that follow
          vallen    varint
          keydelta  unshared bytes
          value     vallen bytes

 trailer: restarts  u32 × n   offsets of restart points
          n         u32
          type      u8        0 = uncompressed
          crc32c    u32       over contents + type
```

**Filter block.** A Bloom filter over the *user* keys in the file (not the
internal keys — a lookup does not know the sequence number it is looking for):

```text
 ┌──────────┬──────────┬──────────┬──────────┬────────────┐
 │ bits/key │ k (u8)   │ nbits u32│ bitmap   │ crc32c u32 │
 └──────────┴──────────┴──────────┴──────────┴────────────┘
```

Default 10 bits per key and k = 7, giving roughly a 1% false-positive rate.
Bit positions come from a single 64-bit hash split into two 32-bit halves and
combined as `h1 + i·h2` (Kirsch–Mitzenmacher), which is as accurate as k
independent hashes at the cost of one. **False negatives are impossible by
construction** (STORAGE-8); false positives cost a wasted block read and
nothing else.

**Index block.** One entry per data block: initially the block's full last
internal key, plus the block's offset and length. The index is sparse — it
locates a block, not a key — so it stays small enough to keep resident. Index
binary search uses the §5.2 internal-key comparator, never raw encoded-byte
comparison. A future user-key-aware shorter separator is permitted only with a
proof that it preserves lower-bound lookup semantics.

**Footer.** Fixed size, read with one 48-byte pread from the end of the file:

```text
 ┌──────────────┬──────────────┬─────────┬──────────────────┐
 │ filter handle│ index handle │ version │ magic u64        │
 │ off+len      │ off+len      │  u32    │ 0x52697665744442 │
 └──────────────┴──────────────┴─────────┴──────────────────┘
```

The magic identifies the file as a RivetDB SSTable; the version supports format
evolution (R10). An unrecognised version is an error, never a guess.

**Ordering within a level.** L0 files may overlap, because they are flushed
MemTables and each covers whatever keys happened to be in memory; they are
searched newest-first. L1 and below are non-overlapping, so a lookup binary
searches the level and reads at most one file.

### 5.4 Manifest and version set

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
manifest describes must match exactly (STORAGE-9); a mismatch is corruption and
is reported.

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
