# ADR-0008: MemTable rotation and flush lifecycle

**Status:** Accepted
**Date:** 2026-09-09
**Phase:** 1F

## Context

Phase 1 has independently durable WAL records, a mutable/frozen MemTable and
atomically published SSTables. Connecting them introduces ownership and crash
boundaries that none can settle alone: sequence assignment, batch visibility,
rotation, bounded immutable memory, flush failure and the meaning of a
physically durable table before a manifest exists.

## Options

### Synchronous flush on the write path

This has little lifecycle state and natural backpressure, but turns ordinary
writes into SSTable fsyncs and prevents new writes while a table is built.

### Parallel background flushes

Parallelism can raise throughput on capable storage, but permits completion
reordering, complicates retry/file identity, and is unmeasured at this stage.

### One FIFO worker with bounded immutable generations

One worker preserves generation order and prevents duplicate concurrent
flushes. A bounded backlog permits writes into the next active table while disk
work proceeds, then applies explicit backpressure instead of growing memory
without limit.

## Decision

Introduce a narrowly scoped `pipeline.Pipeline`. Its serialized write path
assigns one contiguous storage-sequence range, validates and encodes the whole
batch, appends it to a `SyncBatch` WAL, atomically applies it to exactly one
active MemTable, then rotates after the accepted batch when the configured
deterministic size threshold is reached. A batch may exceed the target, but is
never split across generations. Acknowledgement follows WAL fsync and complete
MemTable application; a crash between those stages is recovered by WAL replay
and may leave the client with an ambiguous result.

Every table has a monotonic in-memory generation and every immutable receives
a stable monotonic SSTable file number at rotation. These allocators are not
restart-safe until the manifest persists them in Phase 1G. Rotation freezes the
old active table, installs a fresh active table and queues the old generation
under one coordinator lock.

One owned worker flushes the FIFO head. States are active, queued, flushing,
failed and durable. A failed head remains live and stops later flushes. Explicit
retry is allowed only when no final filename exists; a final filename after an
error is ambiguous and is never removed or overwritten. Successful Phase 1D
publication plus Phase 1E exact-content validation is the Phase 1F condition
for removing an immutable from the live backlog.

The immutable count is bounded. Because whether a batch crosses the threshold
is known only after application, the initial policy conservatively waits for a
free immutable slot before WAL append whenever the backlog is full. This wait
is cancellable and occurs before any durable effect. Once WAL append succeeds,
cancellation cannot turn the result into a false `context.Canceled` response.

Close stops new writes, leaves a nonempty active table recoverable through the
WAL, drains already queued flushes when possible, stops and joins the worker,
then closes the WAL. No goroutine is detached. Phase 1F never deletes or
truncates WAL data after a flush.

## Consequences

- Concurrent callers receive deterministic sequence order through one write
  mutex, while the flush worker performs file I/O without holding it.
- Backpressure can begin before the active table itself reaches its threshold;
  this conservative policy prevents a post-durability cancellable wait.
- A physically durable `.sst` is not yet logically installed across restart.
  Phase 1G must record it in the manifest, persist allocator state and later
  authorize WAL reclamation.
- Snapshot-based frozen iteration allocates a full copied traversal during
  flush. Phase 1F records this cost rather than adding a second MemTable
  ownership mode prematurely.
- Final tables left by a crash or post-rename error are conservative orphans
  for Phase 1G/1I reconciliation.

## Revisit if

Measurements justify parallel flush, byte-based backlog accounting, or a
zero-copy frozen iterator. Each change must retain stable generation identity,
batch-to-generation uniqueness and the manifest/WAL authority boundary.
