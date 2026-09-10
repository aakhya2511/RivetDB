# Phase 1I evidence — integrated local LSM engine

Date: 2026-09-10

## Architecture and semantics

`internal/storage/engine` integrates one Phase 1F WAL/MemTable/flush pipeline,
one Phase 1G Manifest Store and one Phase 1H compaction executor. The local API
is Open, Put, Delete, Get, Scan, Flush, Compact and Close. Writes retain the
single WAL-sync-before-MemTable path. Open validates CURRENT's live Version,
repairs only a proven incomplete WAL tail, replays whole batches above the
inclusive frontier directly into the initial MemTable, and resumes above the
maximum durable sequence without re-appending history.

Get and Scan capture one sequence, active/immutable membership and immutable
Version. Get examines all candidate-bearing sources and selects the greatest
visible sequence. Scan reuses the compaction heap merge, collapses each user-key
version group and omits a newest tombstone. L0 overlap is unrestricted; L1+
point selection is a decoded-user-range binary search. A flush handoff retains
the immutable until its matching SSTable is Manifest-installed. A compaction
reader retains either the old input Version or new output Version, and old
files remain physical.

SSTable readers are opened, fully validated and closed per operation. A bounded
cache is deferred until profiling can justify reference-counted borrowing.
There is no Bloom filter or block cache.

## Correctness evidence

- New create, Put/Delete/Get/Scan, graceful Close and reopen preserve exact
  latest state. Put of an empty value is distinct from Delete.
- Four overlapping L0 versions select the newest; after compaction the same
  result comes from L1. A later tombstone remains not-found through flush and
  older values never resurrect.
- Binary, prefix, `0x00`, `0xff` and empty user keys survive four L0 flushes,
  L1 compaction, point lookup and ordered range collapse.
- A pre-flush MemTable snapshot remains readable after installation. A captured
  pre-compaction Version's four input files remain openable after replacement,
  while a new Version contains only the L1 output.
- A valid orphan table containing `orphan=no` and a stale temp file are reported
  but excluded: Get returns not-found and Scan returns only the Manifest-live
  `live=yes` entry. Missing and byte-corrupt live tables both prevent Open.
- Reopening a WAL-backed active MemTable leaves WAL length unchanged, returns
  the recovered value and assigns the next sequence without reuse. A nonempty
  directory lacking CURRENT is rejected rather than adopted.
- Three 300-operation reference histories use fixed seeds 101 and 9901 plus one
  fresh `testutil.Seed`; each mixes Put/Delete, nine scheduled flush points,
  three compaction attempts and three reopen points, with point and full-scan
  comparison throughout. A separate 40-cycle write/flush/compact/reopen model
  checks logical equality and monotonic file/sequence authority every cycle.
- Concurrency runs two writers (240 total writes), two readers (240 Gets and
  240 Scans), eight flush attempts and eight compaction attempts. Close races
  16 point/range readers. Race count-two and leak checks are part of the gate.
- Read-amplification counters mechanically observe zero table reads for an
  active-only hit, four physical table reads for four overlapping L0 files, one
  for an L1 hit, and one L0 plus one L1 table for a range-covering miss. The
  eight-overlapping-L0 benchmark opens all eight tables.

Lower-layer failure evidence remains authoritative: WAL write/sync poisoning
is in Phase 1B/1F, SSTable write/fsync/rename/directory-fsync failure and
immutable retention are in Phase 1D/1F, Manifest poisoning and pre/post-durable
publication are in Phase 1G, and compaction output/corrupt-input/stale-install
failure is in Phase 1H. Integrated Open maps corrupt/missing authoritative
tables to failure, never not-found; closed operations return `ErrClosed`.

## Benchmark snapshot

Ten-iteration engineering baseline on Apple M4, darwin/arm64, Go 1.25.14.
Put/Delete use synchronous WAL durability. Per-operation SSTable Open performs
full validation. Throughput is derived as `1e9/ns-op`; the Go benchmark harness
does not report P50/P95.

| Benchmark | ns/op | ops/s | B/op | allocs/op |
|---|---:|---:|---:|---:|
| Put | 3,556,775 | 281 | 364 | 12 |
| Delete | 5,922,842 | 169 | 364 | 11 |
| Get active | 4,804 | 208,160 | 116 | 5 |
| Get L0 | 68,962 | 14,501 | 2,380 | 69 |
| Get 8 overlapping L0 | 503,100 | 1,988 | 20,118 | 520 |
| Get L1 | 59,567 | 16,788 | 3,036 | 80 |
| Get miss | 12,604 | 79,340 | 252 | 6 |
| Scan 20 keys | 168,604 | 5,931 | 70,356 | 848 |
| Scan 2,000 keys | 1,015,175 | 985 | 1,740,984 | 26,364 |
| Mixed 70% Get/20% Put/10% Delete | 905,038 | 1,105 | 182 | 9 |
| Open/restart | 591,954 | 1,689 | 17,521 | 182 |
| Write + flush + eligible compaction | 19,984,767 | 50 | 24,382 | 457 |

These are baselines for later profiling, not production or consistency claims.
End-to-end physical write amplification was not measured, so no unqualified
write-amplification claim is made.

## Scope and reclamation

WAL GC candidate computation and physical WAL deletion are deferred. Compaction
tracks logical obsolete SSTables, but physical deletion remains forbidden.
No MVCC transaction/snapshot API, serializable isolation, pruning, tombstone
GC, Raft, replication, distributed transaction, range split/migration,
rebalancing, SQL or AI operator was added.

## Repository gate

- Exact `make check` with Go 1.25.14: pass; the count-two WAL race campaign
  completed in 342.593 seconds.
- Explicit Go 1.27.1 vet, normal tests and count-two race: pass; its WAL race
  campaign completed in 174.059 seconds.
- golangci-lint v2.6.1 under Go 1.25.14: zero issues.
- `go mod tidy`, `git diff --check` and no-`go.sum`/zero-dependency check: pass.
- Test inventory: 225 top-level tests, 150 subtests, 375 total.
