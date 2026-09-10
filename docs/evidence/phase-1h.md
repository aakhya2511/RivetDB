# Phase 1H evidence — version-preserving LSM compaction

Date: 2026-09-10

## Architecture and preservation

`internal/storage/compaction` implements deterministic oldest-seeded L0-to-L1
overlap closure, immutable plans, production-reader input validation, bounded
heap merge, user-key-aware output splitting and one-edit Manifest replacement.
All versions and tombstones are preserved. Exact internal-key duplicates fail
the attempt without logical replacement because the v1 writer cannot encode
duplicates in one output; no silent deduplication occurs.

The default trigger is four L0 files and the default target is 4 MiB of logical
key/value bytes. The picker repeatedly expands across L0 and L1 user-key ranges.
Outputs are allocated through the durable Manifest authority, durably published,
reopened and validated. Installation rechecks every input and the candidate
Version, then appends one edit containing every DeleteFile and AddFile. Replay
frontier fields are absent. Removed inputs are reported obsolete but retained.

## Failure, property and restart evidence

- The closure case `[a,f]`, `[e,k]`, `[j,m]`, `[x,z]` selects the first three
  source files; target `[b,d]` and `[h,q]` join closure while `[r,w]` does not.
- Multi-output integration preserves an exact entry multiset containing four
  `foo` versions and a tombstone, emits at least three ordered non-overlapping
  L1 files, and keeps the entire `foo` group in one output.
- A stale-plan seam changes the Version after outputs are durable. Installation
  rejects it and every output remains unlisted.
- A pre-Manifest stop followed by restart recovers four input files as live and
  all outputs as orphans. A post-Manifest-fsync/pre-volatile-publication test
  recovers the output live and input non-live atomically.
- Exact duplicates fail safely with all four original inputs live.
- A deterministic seed `101` stress creates 100 L0 tables containing
  overlapping ranges, versions and tombstones, compacts until below trigger,
  and compares the exact final global multiset.
- A fresh logged seed repeats the reference-content model over binary-key
  tables so failures remain exactly replayable through `RIVETDB_SEED`.
- Every valid and invalid explicit job-state transition is enumerated.

## Benchmark snapshot

One-iteration engineering baseline on Apple M4, darwin/arm64, Go 1.25.14. Write
amplification is `output SSTable bytes / selected input SSTable bytes`.

| Benchmark | ns/op | throughput | B/op | allocs/op | write amp |
|---|---:|---:|---:|---:|---:|
| 2-way merge | 976,666 | 11.53 MB/s | 329,624 | 4,797 | — |
| 8-way merge | 4,039,167 | 11.15 MB/s | 1,320,264 | 19,176 | — |
| 32-way merge | 15,960,083 | 11.29 MB/s | 5,260,784 | 76,686 | — |
| small compaction | 19,833,709 | — | 754,088 | 10,887 | 0.7736 |
| medium compaction | 71,372,667 | — | 13,644,480 | 167,976 | 0.7642 |
| multi-output compaction | 322,497,834 | — | 6,904,184 | 87,727 | 0.7785 |

These are baselines, not production performance claims.

## Scope

No MVCC pruning, tombstone GC, final multi-level read path, Bloom filter, cache,
WAL GC, physical obsolete-SSTable deletion, Raft or distributed behavior was
implemented.
