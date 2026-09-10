# ADR-0011: Integrated local LSM read/write semantics

**Status:** Accepted
**Date:** 2026-09-10
**Phase:** 1I

## Context

Phases 1A through 1H proved the WAL, MemTable, SSTable, flush, Manifest and
compaction units independently. A local database needs one owner that recovers
and coordinates them without creating a second write path or allowing a read
visibility gap during immutable-table installation.

## Decision

`internal/storage/engine.Engine` owns one Phase 1F pipeline, Manifest Store and
Phase 1H compaction executor. `Open` creates or recovers CURRENT and its
Manifest, validates exactly the live SSTables, repairs only a proven incomplete
WAL tail, recovers sequence/file authority, and applies whole WAL batches above
the inclusive replay frontier directly to the initial active MemTable. Replay
never appends to the WAL. Recovered state may temporarily exceed the MemTable
target; the next write or explicit flush rotates it after the engine is ready.

Writes use only `pipeline.Write`: validate, assign a sequence, append and sync
the WAL, atomically apply the batch to the active MemTable, acknowledge, then
rotate if its target was reached. Put of an empty value remains a value; Delete
stores a tombstone.

Each Get or Scan captures the pipeline's active/immutable membership and latest
sequence under one pipeline lock, then takes one immutable Version reference.
An immutable is not removed until its table is Manifest-installed, so this
ordering admits a short, safe overlap but no gap. That overlap can expose the
same entry from an immutable and its matching installed file number; it is
recognized as the one legal exact duplicate. Any contradictory same-sequence
entry, or an exact duplicate from different authoritative identities, is
corruption.

Get asks every active and immutable MemTable, every overlapping L0 file, and at
most one range-selected table per non-overlapping higher level. The greatest
sequence not above the captured boundary wins; a winning tombstone maps to
not-found. Higher-level selection uses binary search over decoded user-key
ranges.

Scan uses the Phase 1H generic heap merge over MemTable and relevant SSTable
range iterators. It groups internal entries by decoded user key, selects the
newest entry at or below one captured sequence, omits tombstones, and emits one
value per user key in `[start,end)`. This is a sequence-bounded read operation,
not an MVCC transaction or snapshot-isolation API.

Flush performs Phase 1F rotation/drain; the immutable leaves the read set only
after the Store durably installs its L0 file and publishes the new Version.
Compaction is explicit and readers retain their one old or new immutable
Version for the complete operation. Physical deletion remains forbidden.

SSTables are opened, fully validated and closed per operation. A reader cache
is deferred until Phase 1K profiling because safe eviction would require borrow
tracking and would add lifetime complexity before it is justified. There is no
block cache or Bloom filter.

Close stops new engine operations, waits for in-flight calls, drains only
already-immutable flushes, closes the WAL pipeline, then closes the Manifest.
The active MemTable need not flush because acknowledged writes are WAL durable.
Caller cancellation does not abandon cleanup once Close begins. Repeated Close
returns the first close result. Metadata/WAL poisoning makes
writes fail predictably while already-authoritative state remains readable.

## Consequences

RivetDB now has a complete local latest-state KV lifecycle across active,
immutable, L0 and L1 storage and restart. Per-operation table validation is
deliberately expensive, and only L0-to-L1 compaction exists. WAL GC, obsolete
SSTable deletion, MVCC snapshots, pruning, tombstone GC, Raft and distributed
behavior remain outside this decision.
