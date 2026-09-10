# Phase 1C evidence

**Date:** 2026-09-09

**Result:** PASS

**Scope:** Mutable/frozen in-memory ordered table only. No SSTable, Bloom
filter, flush pipeline, manifest, compaction, Raft or MVCC transaction code.

## Design and contracts

[ADR-0005](../design-decisions/0005-memtable-skip-list.md) records the choice
of a custom skip list over a balanced tree, sorted copy-on-write slice and
third-party container. Maximum height is 20 and promotion probability is 1/4.
The production topology source is concurrency-safe; tests inject seeded
sources. Topology does not affect logical results.

All internal ordering and lower bounds call `storage.CompareInternal`. Exact
internal-key duplicates replace their values; a different sequence or kind is
a separate entry. Inputs are copied, and returned values do not alias table
storage.

A single RW mutex serializes inserts and freeze and permits concurrent point
reads. Iterators copy an ordered snapshot under a read lock and then require no
lock or close. Freeze is permanent and idempotent; an insert racing with freeze
is wholly accepted before it or returns `ErrFrozen`.

## Correctness evidence

- Deterministic cases cover empty and arbitrary binary keys, empty/prefix/long
  prefix keys, `00`/`ff`, sequence extremes, descending versions, delete before
  value at equal sequence, delete versus empty PUT, and the ADR-0003 raw-byte
  prefix counterexample.
- Exact get, comparator lower-bound seek, version-candidate seek, full and
  lower-bound iteration, half-open user-key ranges, unbounded ranges,
  exhaustion, replacement and freeze are directly tested.
- Six reference-model seeds perform 5,000 inserts/replacements each. Periodic
  checks compare exact values, entry count, full sorted order, exact get,
  general seek and version-candidate seek against a slice sorted only with the
  authoritative comparator.
- A separately invoked fixed seed performs 100,000 operations with structural
  validation every 10,000 operations and at completion. Validation checks the
  level-zero set and strict order, every higher ordered subsequence, cycles,
  node levels, exact-key uniqueness, count and memory accounting.
- Ownership tests mutate the source user-key/value buffers and returned value;
  stored ordering and bytes remain unchanged.

## Concurrency evidence

The race stress uses 128 writers × 100 unique entries plus 16 concurrent
readers repeatedly mixing exact get, seek and iterator snapshot construction.
It verifies all 12,800 entries, global iteration order and structure. A
separate 128-writer insert/freeze race checks accepted-count equality, permanent
post-freeze rejection and post-freeze iteration. Both passed twice under the
race detector.

## Memory accounting

`SizeBytes` is a deterministic approximation. It includes table and head-node
overhead, node metadata, copied user-key bytes, retained value capacity and all
forward-link slots. Exact replacement reuses shorter buffers and charges only
growth, so size never decreases. Addition saturates at `uint64` maximum.
Tests assert the empty baseline, known height-one and height-two inserts, short
and growing replacement, 4 KiB keys, 8 KiB values, inclusive threshold
reporting, overflow saturation and post-freeze stability. Iterator-owned
snapshot allocations are deliberately excluded.

## Benchmark baseline

Measured with Go 1.25.14 on darwin/arm64, Apple M4, `-benchmem
-benchtime=50ms`. Insert and iteration numbers are for a complete table-sized
batch/scan, not one element:

| Benchmark | Entries | ns/op | B/op | allocs/op |
|---|---:|---:|---:|---:|
| Insert sequential | 1,000 | 135,145 | 131,265 | 5,006 |
| Insert sequential | 10,000 | 1,723,485 | 1,307,799 | 50,006 |
| Insert sequential | 100,000 | 19,992,667 | 13,067,064 | 500,006 |
| Insert random | 1,000 | 171,239 | 144,414 | 5,006 |
| Insert random | 10,000 | 2,772,903 | 1,440,106 | 50,006 |
| Insert random | 100,000 | 51,267,917 | 14,399,896 | 500,007 |
| Seek hit | 100,000 | 166.6 | 8 | 1 |
| Seek miss | 100,000 | 70.59 | 0 | 0 |
| Forward iteration | 100,000 | 6,604,182 | 36,111,554 | 200,030 |
| Mixed version candidate | 100,000 | 225.6 | 24 | 3 |

These are development baselines, not performance claims. Snapshot iteration's
copying cost is intentionally visible and is a future profiling candidate.

## Repository gate

- Go 1.25.14 `make check`: PASS (format, vet, golangci-lint v2.6.1,
  normal tests and `-race -count=2`).
- Go 1.27.1 vet, normal tests and `-race -count=2`: PASS.
- Normal Go 1.25.14 suite: 179 passing test/subtest events; the opt-in
  100,000-operation structural test passed separately with seed 48392017.
- `go mod tidy` and `git diff --check`: PASS with no changes or whitespace
  errors.
- `go list -m all`: only `github.com/rivetdb/rivetdb`; zero external
  dependencies and no `go.sum`.

golangci-lint is reported only under Go 1.25.14 because v2.6.1 cannot read Go
1.27 export data.
