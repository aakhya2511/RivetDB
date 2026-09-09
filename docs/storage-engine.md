# Design Note: Local Storage Engine (Phase 1)

**Status:** design, not implemented. This note specifies what Phase 1 builds.
It is written before the code so the on-disk format is a decision rather than
an accident.

**Related:** [ADR-0001](design-decisions/0001-lsm-tree-over-b-tree.md) ·
[invariants.md](invariants.md) (STORAGE-1 … STORAGE-11) ·
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
integers are LEB128. Checksums are **CRC32C** (Castagnoli), chosen for hardware
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

A WAL segment is a sequence of 32 KiB blocks. Records are framed within blocks
and fragmented across them when needed:

```text
 0        4        6      7                      7+len
 ┌────────┬────────┬──────┬──────────────────────┐
 │ CRC32C │ length │ type │        payload       │
 │  u32   │  u16   │  u8  │       len bytes      │
 └────────┴────────┴──────┴──────────────────────┘

 type:  1 FULL    the whole record is in this fragment
        2 FIRST   the first fragment of a record
        3 MIDDLE  a middle fragment
        4 LAST    the final fragment
```

The CRC covers the type byte and the payload. A block's trailing bytes are
zero-padded when fewer than 7 remain, and a reader skips the padding.

*Why block framing rather than a plain length-prefixed stream:* corruption in a
length-prefixed stream is unrecoverable, because a corrupt length makes every
subsequent offset wrong. With fixed blocks, a reader that hits a bad record
resynchronises at the next block boundary. This matters most for the manifest,
where losing the tail loses the database.

The payload of a FULL/FIRST fragment is a write batch:

```text
 ┌───────────────┬───────┬──────────────────────────────┐
 │ first seq u64 │ count │  count × (kind, key, value)  │
 └───────────────┴───────┴──────────────────────────────┘

 entry:  kind u8   (0 = delete, 1 = value)
         key       varint length + bytes
         value     varint length + bytes (absent when kind = delete)
```

A batch is the unit of atomicity: replay applies all of it or none of it.

**Recovery** reads segments in order, applies each intact batch to a fresh
MemTable, and stops at the first record that fails its checksum or is
incomplete. Everything after that point is discarded, because a crash can only
have truncated the tail (STORAGE-3). A checksum failure in the *middle* of a
segment is different — that is corruption, not truncation — and is reported as
an error rather than silently truncated (STORAGE-2).

### 5.2 Internal keys

Every key stored in a MemTable or SSTable carries a trailer:

```text
 internal_key = user_key ‖ BE64(^sequence) ‖ kind_u8

   sequence  monotonically increasing, unique per write
   ^         bitwise complement
   kind      0 = deletion (tombstone), 1 = value
```

The complement is the point. With it, a plain `bytes.Compare` sorts by user key
ascending and, within one user key, by sequence **descending** — newest first.
So no custom comparator is needed anywhere: the ordering invariant (STORAGE-4)
is checkable with `bytes.Compare`, and a seek to `(key, ^seq)` lands directly on
the newest version visible at that sequence.

This is also the hook for MVCC (R5). In Phase 5 commit timestamps map onto the
sequence space, and "read at timestamp T" becomes "seek to `(key, ^T)` and take
the first entry" — no change to the file format.

A tombstone is an ordinary entry with `kind = 0` and no value. It shadows older
versions until compaction can prove they are unreachable.

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

**Index block.** One entry per data block: the block's last key (or a shorter
separator between it and the next block's first key, which shrinks the index)
and the block's offset and length. The index is sparse — it locates a block,
not a key — so it stays small enough to keep resident.

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
| `SyncInterval(d)` | fsync at most every `d` | up to `d` of writes | throughput-oriented |

`SyncBatch` is the default because the engine's main consumer is a Raft log,
where "acknowledged" must mean "durable" or RAFT-10 is violated. The other
modes exist so that benchmarks can quantify what fsync costs, and the mode in
use is always reported alongside a benchmark number.

Process-crash versus machine-crash is distinguished explicitly: a write in the
page cache survives a process crash but not a power loss, and a document that
conflates the two is describing a guarantee it does not have.

## 7. Failure scenarios

| Scenario | Handling | Invariant |
|---|---|---|
| Crash mid-WAL-append | the torn trailing record fails its checksum and is truncated; earlier records replay | STORAGE-3 |
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
- WAL record framing: fragmentation across block boundaries, padding, resync
  after a corrupt record.
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
