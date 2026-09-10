# ADR-0013: Evidence-driven local-storage performance architecture

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 1K

## Context

Phase 1J established crash, visibility and reclamation correctness, but the
integrated read path reopened and fully validated each SSTable on every
operation and flush copied a frozen MemTable into a full snapshot. Phase 1K's
rule is that an optimization needs a baseline, a profile, an A/B measurement
and a correctness rerun. Persistent formats and the Phase 1J durability and
visibility contracts are not performance tuning knobs.

The available APFS data volume was 96% utilized. Disk-sensitive measurements
are therefore constrained-environment baselines, not representative device
claims. CPU/allocation profiles and same-machine implementation comparisons
remain useful with that qualification. `RIVETDB_BENCH_DIR` allows benchmark
data to move to an external local SSD without moving the repository.

## Decision

Retain four narrowly measured changes:

1. A frozen MemTable may be traversed directly through its immutable level-zero
   chain. Values are copied per entry and `InternalKey` has private byte state,
   so the iterator never exposes mutable MemTable storage. Public snapshot
   iterators are unchanged.
2. Each Engine owns a thread-safe, 32-entry default SSTable reader cache keyed
   by the never-reused file number within that Engine directory. Admission
   still calls eager `sstable.Open`, including complete data validation. A
   lease prevents eviction; capacity pressure uses uncached leased readers
   when every retained entry is borrowed. Engine Close holds its exclusive
   operation lock while draining the cache. Reclamation evicts or waits for the
   lease before unlinking an obsolete file.
3. Each new v1 SSTable activates the already-reserved user-key Bloom block:
   deterministic ten bits/key and seven probes. Eager Open cross-checks every
   stored user key against it, so a checksum-valid false negative is corruption.
   Older v1 tables with the reserved handle absent remain compatible.
4. Scan transfers already operation-owned iterator key/value bytes to its
   materialized result and stores its selected candidate by value. No public
   result aliases an SSTable block, cache buffer or MemTable node.

The Manifest/Version remains the logical authority. A cache hit is physical
reuse of an immutable file identity, not an alternate Version lookup.

The test architecture is split into normal, race, stress, subprocess-crash and
every-byte exhaustive tiers. `make certify-local` runs every correctness tier;
normal CI stays practical and scheduled/manual CI runs the heavy tiers.

## Alternatives

### Lazy SSTable validation

This would reduce first-open work, especially startup. It was rejected because
eager validation is the established Phase 1E trust boundary. Proving lazy
cross-block ordering and corruption behavior would add more correctness
surface than the measured cache needed. The cache amortizes eager validation.

### Block cache

After the table cache, repeated checked data-block reads are the point-read CPU
hotspot. A block cache would improve that workload, but it would also change
the tested rule that post-open file corruption is surfaced on a later access.
It is deferred until an integrity model can preserve or deliberately replace
that guarantee.

### Group commit

Synchronous write profiles are dominated by durability syscalls, and a
multi-mutation `WriteBatch` demonstrates amortization. A background grouping
protocol was rejected for Phase 1K because Phase 2 must first decide whether
the Raft log, the data WAL, or both are durability authorities. Adding group
commit now risks optimizing a double-log architecture that should not exist.

### WAL segmentation, Version refcounts and format retuning

Retained-WAL replay is linear and reaches roughly 1.23 seconds at 100,000
batches on this machine, but the single file remains correct. Segmentation is
a crash-safety subproject and is deferred. Lifecycle-lock mutex profiles showed
no material contention, so Version refcounts were rejected. 8/16 KiB data
blocks made point lookup materially worse for small file-size savings, so the
4 KiB default remains. No block/restart encoding or format version changed;
decision 3 deliberately populates the optional filter section already reserved
by SSTable v1, so newly written table bytes do differ while older filterless v1
tables remain readable.

## Consequences

Repeated reads avoid repeated complete-file validation while retaining the
same validation boundary. Cache capacity bounds retained descriptors, leases
extend physical lifetime explicitly, Bloom skips definite misses, flush peak
allocation falls, and Scan does fewer redundant copies. The Engine still has
one WAL, no block cache, synchronous per-batch durability,
version/tombstone retention, and no MVCC, Raft or distributed behavior.

Phase 2 must introduce a replicated-state-machine apply boundary. Calling
`Engine.Put` independently on replicas is not that boundary: it assigns local
sequences and appends the local data WAL. Raft-log versus data-WAL authority,
deterministic logical sequence ownership and snapshot export/install are Phase
2/3 design decisions.
