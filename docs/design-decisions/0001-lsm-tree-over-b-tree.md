# ADR-0001: LSM tree over B+ tree for the storage engine

**Status:** Accepted
**Date:** 2026-09-09
**Phase:** 1

## Context

RivetDB needs a local ordered key-value store that it owns end to end. Two
structures are viable, both used by serious production databases: a B+ tree
(PostgreSQL, InnoDB, LMDB, BoltDB) and an LSM tree (LevelDB, RocksDB, Cassandra,
ScyllaDB, and the storage layers of TiKV and CockroachDB).

Neither is universally better. The choice depends on the workload and on what
the layers above need from the storage layer, so the question is specifically
what *RivetDB's* layers need.

Three properties of this system shape the answer:

1. **The write path is a Raft apply loop.** Committed entries are applied in
   order by a single goroutine. The engine sees a serialised, write-heavy,
   append-oriented stream — not a general mixed workload with concurrent
   writers.
2. **MVCC (Phase 5) writes versions, never updates in place.** Every logical
   update becomes a new `(key, timestamp)` entry, and old versions are removed
   in bulk by garbage collection. The workload is append-and-then-bulk-delete.
3. **Range splitting (Phase 7) is the project's signature feature.** Splitting a
   range must not require rewriting or copying its data, because doing so would
   make the operation's cost proportional to range size and turn "online split"
   into a long stall.

## Options

### Option A: B+ tree

Fixed-size pages, updated in place, with a write-ahead log for atomicity and
crash recovery.

**Advantages, stated fairly:**

- Reads are excellent and, more importantly, *predictable*: a lookup is one
  root-to-leaf traversal, so the depth bounds the cost. No Bloom filters, no
  merging across levels, no dependence on how recently compaction ran.
- Range scans are sequential over leaf pages linked in key order.
- Space amplification is low. There are no obsolete versions waiting for
  compaction to reclaim them.
- No background compaction, so no compaction-induced latency spikes and no
  background I/O competing with foreground requests.
- Simpler mental model for crash recovery: a page is either the old one or the
  new one.

**Disadvantages for this system:**

- Writes are random I/O. A logical write dirties a page, and pages are scattered
  across the file; sustained write throughput is bounded by random write
  performance.
- Write amplification at the page level is severe for small values: updating a
  100-byte value dirties a 4 KiB or 8 KiB page.
- In-place updates require locking or copy-on-write page management to keep
  concurrent readers consistent, which is exactly the complexity an MVCC layer
  above would be duplicating.
- Splitting a range means copying its keys into a new tree. There is no way to
  share a subtree between two trees without a copy-on-write page manager with
  reference counting — which is a substantial amount of machinery to build.

### Option B: LSM tree

Buffered writes in a sorted in-memory structure, flushed to immutable sorted
files, merged by background compaction.

**Advantages for this system:**

- Writes are sequential: a WAL append plus an in-memory insert. This matches the
  Raft apply loop directly, and it is the operation that has to be fast.
- Immutable files mean readers need no locks at all. A reader takes a reference
  to a version and is never blocked by a writer or by compaction. Much of the
  concurrency complexity disappears rather than being managed.
- Deletes are tombstones — cheap writes — and MVCC's bulk version cleanup is
  absorbed by a compaction pass that was going to happen anyway. The garbage
  collection Phase 5 needs is *already* the compaction Phase 1 builds.
- **Immutable files can be referenced by two ranges at once.** A split can hand
  both children a reference to the same SSTable with different key bounds, and
  let compaction separate them later. This makes an online split a metadata
  operation rather than a data copy — decisive for requirement 3.
- The multi-version key encoding falls out naturally: entries are already
  `(key, sequence)` sorted newest-first, so "read at timestamp T" is a seek.

**Disadvantages, stated fairly:**

- Read amplification: a lookup may consult several levels. Bloom filters make
  the common case one block read, but a miss on a key that exists in a deep
  level costs more than a B+ tree lookup would.
- Compaction is background work that competes with foreground I/O and produces
  latency variance. It has to be rate-limited and measured.
- Space amplification from obsolete versions awaiting compaction.
- More moving parts: MemTable, immutable MemTable, levels, manifest, version
  set, compaction picker. More code, so more places to be wrong.

## Decision

**An LSM tree.**

The deciding factor is not the classic write-throughput argument — it is
immutability. Two of RivetDB's hardest requirements are made substantially
easier by files that never change after they are written:

- Range splitting becomes a metadata operation instead of a data copy, which is
  what makes it plausible to do online (Phase 7).
- Lock-free readers concurrent with compaction fall out of the design, rather
  than being achieved through page-level concurrency control.

The workload argument reinforces it: an append-only MVCC version stream with
bulk version cleanup is close to the ideal LSM workload, and the Raft apply
loop's serialised write stream removes the multi-writer complexity that would
otherwise be an LSM weakness.

The cost is read amplification and compaction variance. Both are accepted, both
are bounded by design choices documented in
[storage-engine.md](../storage-engine.md) (levelled compaction, Bloom filters),
and both are measured in the Phase 1 benchmark rather than assumed away.

## Consequences

- The engine must implement compaction correctly, including the subtle case:
  dropping a tombstone is only safe when no older version of that key can
  survive in any remaining file and no live reader is positioned before it.
  This is invariant STORAGE-6 and has a dedicated test.
- Read latency depends on the shape of the level structure, so benchmarks must
  report the state of the LSM (level sizes, L0 file count) alongside latency
  numbers. A benchmark run immediately after a full compaction would flatter
  the system.
- Space and write amplification must be measured and published, since they are
  the honest cost of this choice.
- Background compaction means the engine owns goroutines, so it needs explicit
  lifecycle management and leak detection (enforced by `testutil.NoLeaks`).
- Deterministic testing requires compaction to be triggerable explicitly rather
  than only by background timing.

## Revisit if

- Measurement shows read amplification dominating the workloads RivetDB
  actually targets, and Bloom filter tuning does not recover it.
- Phase 7 finds that sharing SSTables across ranges is impractical for a reason
  not yet foreseen — removing the main argument for this choice.
- Compaction latency variance turns out to make the rebalancer's telemetry too
  noisy to act on, since P99 latency is one of its input signals.
