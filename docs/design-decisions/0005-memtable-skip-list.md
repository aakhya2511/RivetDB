# ADR-0005: Skip list for the MemTable ordered structure

**Status:** Accepted
**Date:** 2026-09-09
**Phase:** 1C

## Context

The MemTable is the first mutable ordered structure between the WAL and future
immutable SSTables. It must retain every internal-key version, provide exact
lookup and comparator lower-bound seek, iterate in flush order, become
permanently immutable, and report a deterministic approximate size. Arbitrary
binary and prefix-related user keys make the explicit comparator from
ADR-0003 load-bearing.

Phase 1C permits concurrent callers even though the assembled Phase 1 engine
will serialize its write path. Correctness and a clear synchronization
contract matter more than lock-free access at this stage.

## Options

### Sorted slice with copy-on-write publication

A sorted slice has excellent locality and trivial iteration. Copy-on-write
snapshots also make readers simple. Insertion is O(n), however, and copying the
whole table for every write makes it unsuitable as the active write buffer.

### Custom balanced search tree

A red-black or AVL tree gives guaranteed O(log n) lookup and insertion and can
support ordered iteration. Its rotations, parent/child invariants and iterator
interaction create substantially more correctness surface. Go's standard
library has no suitable ordered tree, so RivetDB would own all of that code.

### Third-party ordered container

A mature package could reduce implementation work. It would add the module's
first dependency for a small core data structure, while comparator, ownership,
freeze and memory-accounting semantics would still require RivetDB-specific
wrapping and proof.

### Custom skip list

A skip list provides expected O(log n) insertion and seek, O(1) forward
iteration steps, and an already-sorted level-zero chain suitable for a future
SSTable flush. Its mutation algorithm is localized to predecessor links and
does not require tree rotations. Topology affects performance only; logical
results remain entirely determined by the authoritative comparator.

## Decision

Use a custom skip list with maximum height 20 and promotion probability 1/4.
Production construction draws topology values from Go's concurrency-safe
random source. Tests inject a deterministic source. No fixed topology is used,
and the random source never participates in comparison or visible semantics.

Every traversal calls `storage.CompareInternal`; encoded internal keys are
never compared as raw bytes. An exact duplicate internal key atomically
replaces its value. A different sequence or kind remains a distinct entry.

One `sync.RWMutex` protects topology, values, entry count, size and frozen
state. Inserts are mutually exclusive. Point reads may run concurrently.
Iterator construction takes a read lock and copies a complete ordered snapshot,
then releases the lock, so later inserts or replacements cannot change an
existing iterator. This intentionally spends O(n) reader-local memory for
simple, stable semantics; frozen-table iteration can be optimized later only
if measurement justifies it.

`Freeze` takes the write lock and permanently rejects later inserts. An insert
racing with freeze linearizes by lock acquisition: it either finishes before
freeze or returns `ErrFrozen`. Repeated freeze is harmless.

The table copies all inserted key and value bytes and returns copies. Exact-key
replacement reuses the existing value buffer when possible and grows it when
necessary. The deterministic size estimate includes table/head overhead, node
metadata, owned key bytes, retained value capacity and forward-link storage.
It saturates at `uint64` maximum rather than wrapping. Reader snapshots are not
table-owned and are excluded.

## Consequences

- Insert, exact get, lower-bound seek and version-candidate lookup are expected
  O(log n); a pathological random topology can degrade them to O(n).
- Full and range iterator construction is O(n) in the selected entries;
  `Next` is O(1). Table memory is O(n), with expected 4/3 forward links per
  node at promotion probability 1/4.
- Coarse locking makes operations linearizable and race-free but serializes
  writers and briefly blocks them while an iterator snapshot is copied.
- Replacing a value with a shorter value retains the node's existing value
  capacity. This keeps the size estimate monotonic and avoids allocator churn.
- Level zero is the exact future SSTable flush order, but Phase 1C implements
  no flush scheduler or SSTable code.

## Revisit if

Profiles at realistic MemTable sizes show iterator snapshot copying or coarse
locking dominates storage-engine latency, or adversarial topology becomes part
of the threat model. Any replacement must preserve ADR-0003 ordering, snapshot
iteration, ownership, freeze and deterministic accounting contracts.
