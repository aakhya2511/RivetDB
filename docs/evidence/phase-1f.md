# Phase 1F evidence

**Date:** 2026-09-10

**Result:** PASS

**Scope:** Serialized synchronous-WAL writes, atomic MemTable batch apply,
active rotation, bounded FIFO immutable flush, exact SSTable validation,
failure retention/retry, WAL replay and graceful shutdown. No manifest,
VersionSet, WAL garbage collection, compaction, final read path, Bloom filter,
cache, Raft, MVCC transaction or distributed behavior.

## Architecture and durability evidence

[ADR-0008](../design-decisions/0008-memtable-rotation-and-flush-lifecycle.md)
records the single-worker bounded-FIFO decision. A write is acknowledged only
after `SyncBatch` append and atomic MemTable `ApplyBatch`; rotation follows the
complete batch. Shutdown takes the write admission token before stopping new
writes, drains existing immutables, joins the worker and closes the retained
WAL. A focused crash model fsyncs a batch without applying it and reconstructs
both entries through the same `ApplyBatch` path.

## State, failure and filesystem evidence

The lifecycle table enumerates all 25 state pairs and accepts only the five
documented transitions; four 10,000-operation models (fixed seeds `0`, `1`,
`-1` and fresh seed `9102614309944001338`) repeat the oracle.
Injected WAL failure changes neither sequence nor MemTable. Flush models cover
writer, file-sync, close, rename, directory-sync and reader-validation errors.
The failed FIFO head remains live. Explicit retry reuses its exact generation
and file number. Ambiguous post-rename results reject retry and never remove a
final table.

A real-filesystem integration writes a value version, tombstone and binary key
across three forced rotations. Each Phase 1D output is reopened by the Phase 1E
reader and exactly matches its immutable generation by sequence, key, kind and
value. The same three batches replay from the still-present WAL before and
after clean close. A separate clean-close test leaves a nonempty active table
unflushed and reconstructs its acknowledged entry from WAL.

## Concurrency and backpressure evidence

Sixty-four concurrent writers receive unique sequences and force 64
generations. The accepted-write oracle finds every sequence/key exactly once
across 64 FIFO flushes, with 64 WAL records and no duplicate successful flush.
A blocked first flush allows a second generation, fills a bound of two, and
causes a cancelled third writer to return before its WAL append. Releasing the
worker flushes generations 10 then 11 and permits the third write. An explicit
write-versus-rotation race preserves its one accepted entry. The race gate and
goroutine leak check cover worker shutdown.

## Benchmark baseline

Measured with Go 1.25.14 on darwin/arm64, Apple M4, `-benchmem
-benchtime=20x`. These short engineering baselines are not performance claims:

| Operation | ns/op | B/op | allocs/op | Extra |
|---|---:|---:|---:|---:|
| Single writer, no rotation | 1,325 | 435 | 10 | — |
| Single writer, every-write rotation | 5,054 | 2,091 | 24 | 20 rotations |
| Multiple writers | 2,271 | 535 | 13 | serialized admission |
| Real Phase 1D/1E flush, 4 KiB value | 36,303,456 | 63,785 | 137 | 0.11 MB/s |
| Writes while one flush is blocked | 1,410 | 427 | 10 | no rotation during sample |
| Bound-one backpressure | 6,333 | 2,124 | 25 | 19 events |

The existing snapshot iterator is visible in flush allocation. This phase
records the cost and does not add a zero-copy ownership mode without a larger
representative profile.

## Repository gate

Go 1.25.14 and Go 1.27.1 vet, normal tests and race tests twice passed. The
pinned Go 1.25.14 golangci-lint v2.6.1 gate, formatting, module tidiness and
whitespace checks passed. The module retains zero external dependencies and no
`go.sum`. The normal suite reported 289 passing test/subtest events.
