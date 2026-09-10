# Phase 1K evidence: measured local-storage performance and certification

Date: 2026-09-10. Baseline source: clean Phase 1J commit
`2d84de1a0a39250d4b060e27b059b7f611b62fe4`.

## Measurement environment and limitation

| Item | Recorded value |
|---|---|
| Hardware | MacBook Air `Mac16,12`, Apple M4, 10 cores (4 performance/6 efficiency), 16 GB RAM |
| OS | macOS 15.7.4 (24G517), Darwin arm64 |
| Filesystem | local journaled APFS data volume, 228 GiB capacity |
| Free/utilization | 7.8–8.3 GiB free/~96% during measurements; 15 GiB/~93% at final certification |
| Go | 1.25.14 for measurements; 1.27.1 compatibility gate |
| GOMAXPROCS | unset; Go default 10 (`getconf _NPROCESSORS_ONLN=10`) |
| Ordinary temporary root | `/var/folders/.../T` |
| Measurement root | `/private/tmp/rivetdb-phase1k-bench` on the same APFS data volume |
| External SSD | none mounted during this work |
| Power | initially AC-reported while battery was discharging; later AC and charging |

The filesystem condition does not block correctness or CPU/allocation work.
All internal-disk fsync, flush, compaction, startup and physical-read timing in
this document is a **CONSTRAINED-ENVIRONMENT BASELINE**. It is useful for
diagnosis or a same-machine directional comparison, but is not final
representative device performance. No arbitrary free-space threshold is part
of the benchmark API. A future final disk run must record the same fields and
its power state.

Benchmarks create only owned temporary children and remove only those children.
Setting, for example,
`RIVETDB_BENCH_DIR=/Volumes/<volume>/rivetdb-bench` places benchmark database
data on an external local SSD without moving the repository or hard-coding a
volume name.

## Method

Microbenchmarks use fixed deterministic datasets, actual engine integrity
logic, Go 1.25.14, `-benchmem`, five 750 ms samples unless stated otherwise,
and report the median. A/B samples use the same machine and code except for the
named change. Startup scaling uses three one-iteration samples because setup is
outside the timed operation. Profiles use 2–5 second benchmark windows and
`go tool pprof -top`; raw profile binaries live in `/private/tmp` and are not
committed.

Engine settings are a 64 MiB benchmark MemTable, four immutable tables, a 4
MiB target file and the named L0 trigger. Keys are deterministic sequential
ASCII unless a benchmark says otherwise; values are fixed bytes. Point-read
benchmarks are warm after the first timed cache admission. Put/Delete and
`WriteBatch` use the production synchronous per-batch WAL durability path.
Microbenchmark `ns/op` is not a workload latency percentile. Final P50/P95/P99
disk latency is deferred because the environment cannot support a
representative claim.

Representative commands:

```text
RIVETDB_BENCH_DIR=/private/tmp/rivetdb-phase1k-bench/data \
go test -run '^$' -bench '<workload>' -benchmem -count=5 -benchtime=750ms <package>

go test -run '^$' -bench '^BenchmarkGetL0EightOverlap$' -benchtime=5s \
  -cpuprofile=/private/tmp/rivetdb-getl0.cpu \
  -memprofile=/private/tmp/rivetdb-getl0.mem ./internal/storage/engine
```

## Baseline and A/B results

Phase 1J already recorded integrated synchronous baselines: Put 3.557 ms,
Delete 5.923 ms, mixed 905.0 µs, Open/restart 592.0 µs and write + flush +
eligible compaction 19.985 ms. They remain engineering context, not current
disk claims. The untouched Phase 1J read baseline was rerun before source edits.

| Workload | Before median | After median | Before allocation | After allocation | Interpretation |
|---|---:|---:|---:|---:|---|
| Get active | 519.8 ns | 481.1 ns | 112 B/5 | 112 B/5 | unchanged path |
| Get one L0 | 47.553 µs | 8.251 µs | 2,361 B/69 | 832 B/31 | cached eager reader |
| Get eight overlapping L0 | 516.120 µs | 81.565 µs | 19,864 B/520 | 7,636 B/216 | cached eager readers |
| Get L1 | 56.591 µs | 10.065 µs | 3,017 B/80 | 848 B/31 | cached eager reader |
| Scan 20/100 stored keys | 122.033 µs | 68.302 µs | 70,353 B/848 | 38,084 B/506 | reader cache only |
| Scan 2,000 keys | 2.344 ms | 2.015 ms | 1,739,523 B/26,363 | 1,157,723 B/20,188 | reader cache only |
| Snapshot iteration, 10k | 4.268 ms | 521.0 µs | 3,025,829 B/20,020 | 80,016 B/10,001 | frozen direct iterator |
| Snapshot iteration, 100k | 26.727 ms | 6.102 ms | 36,111,557 B/200,030 | 800,017 B/100,001 | frozen direct iterator |
| Scan 10k before/after owned-transfer change | 13.606 ms | 12.753 ms | 6,516,349 B/100,822 | 5,588,119 B/71,152 | profile sample vs five-sample median |

The timing directions are not final claims because power changed during the
session. Allocation changes are deterministic structural evidence. The table
cache retains eager full validation on first admission; it does not implement
lazy validation.

### Current read and scan characterization

| Operation | Median | B/op | allocs/op | SSTables consulted |
|---|---:|---:|---:|---:|
| active hit | 481 ns | 112 | 5 | 0 |
| one-L0 hit | 8.251 µs | 832 | 31 | 1 L0 |
| four-overlap L0 hit | 39.658 µs | 3,729 | 111 | 4 L0 |
| eight-overlap L0 hit | 22.471 µs | 7,635 | 216 | 8 L0, 0 Bloom skips |
| eight-overlap in-range miss | 5.864 µs | 6,264 | 94 | 8 L0, 8 Bloom skips |
| L1 hit | 9.542 µs | 848 | 31 | 1 higher level |
| full range-pruned miss | 707 ns | 248 | 6 | 0 |

The read-amplification metric is the number of range-selected SSTables whose
`GetCandidate` is called. It is distinct from latency: the benchmarks report
0, 1, 4 or 8 exactly. `TableOpens/op` tends toward zero after warm cache
admission. Bytes read per Get are not yet instrumented and are not claimed.

| Full scan results | Median | B/op | allocs/op |
|---|---:|---:|---:|
| 10 keys | 24.050 µs | 6,507 | 107 |
| 100 keys | 142.361 µs | 50,814 | 743 |
| 1,000 keys | 597.665 µs | 439,578 | 7,104 |
| 10,000 keys | 12.753 ms | 5,588,119 | 71,152 |

### Constrained write, flush and compaction observations

| Operation | Median/observed | Allocation |
|---|---:|---:|
| Put, small | 5.513 ms | 271 B/11 |
| Put, 1 KiB | 6.590 ms | 2,416 B/11 |
| Put, 4 KiB | 5.879 ms | 9,204 B/11 |
| Delete | 6.787 ms | 265 B/10 |
| `WriteBatch`, 16 mutations | 10.272 ms/batch | 4,351 B/119 |
| `WriteBatch`, 256 mutations | 7.802 ms/batch | 68,427 B/1,800 |
| write + flush + eligible compaction | 47.341 ms | 28,971 B/526 |
| medium compaction profile sample | 65.359 ms | 13,656,396 B/168,099 |

These are **CONSTRAINED-ENVIRONMENT BASELINES**. The non-monotonic value-size
and batch timings illustrate why they are not representative. The profile,
not the headline time, establishes that synchronous durability syscalls
dominate writes. The production durability mode was never disabled.

Merge baselines before implementation changes were 659.7 µs/15–22 MB/s for
2-way, 3.415 ms/10–15 MB/s for 8-way and 16.846 ms/10–19 MB/s for 32-way,
with 4,798, 19,177 and 76,688 allocations respectively. No merge algorithm
change was retained. The existing compaction metric is renamed in this phase's
interpretation as `compaction output/input bytes`; it is not end-to-end write
amplification. The medium sample's output/input ratio was 0.7642.

End-to-end local write amplification is defined for future measurement as:

```text
(physical WAL bytes + flush SSTable bytes + compaction output bytes
 + Manifest bytes) / logical mutation key-and-value bytes
```

It includes successful durable representations and excludes temporary failed
outputs, filesystem metadata and fsync traffic. It was not instrumented in
Phase 1K, so no numeric end-to-end or space-amplification result is claimed.

### Format/configuration experiments

Before Bloom activation, a deterministic 20,000-entry table with 4/8/16 KiB blocks produced 504,297 /
502,420 / 499,042-byte files and 123/62/31 data blocks. Median candidate lookup
was 8.230/25.920/43.033 µs with 5,291/8,893/17,506 B per operation. The small
file-size reduction does not justify the point-read loss; the 4 KiB default is
retained. L0 read amplification grows exactly with overlapping file count, so
the trigger remains 4 rather than trading more reads for disk-sensitive write
results that this environment cannot validate. Compaction output-target
tuning is likewise deferred; no default was changed.

The deterministic Bloom property campaign inserted 10,000 user keys and
observed zero false negatives and 75/10,000 false positives (0.75%). Its
payload was 12,516 bytes, or 1.2516 bytes/key including the 16-byte header.
An eight-overlap in-range miss changed from a pre-Bloom median 79.539 µs,
7,924 B and 238 allocations to 5.864 µs, 6,264 B and 94 allocations, with all
eight table reads avoided. Timings remain constrained-environment values; the
skip count and zero-false-negative property are deterministic.

## Profile findings

| Workload | Evidence | Root cause/decision |
|---|---|---|
| L0 eight-overlap before | `sstable.Open` 78.9% cumulative CPU; syscalls 94.5% flat; 654 MB allocation profile led by table selection/open/cloning | repeated eager open/complete validation; add bounded reader cache |
| L0 eight-overlap after | 87.4% CPU in checked `pread`; allocation led by `tablesForPoint`/Manifest copies | table-open hotspot removed; block cache deferred to preserve post-open-corruption detection |
| SSTable GetCandidate | 59.4% CPU in `pread`; 90.3% allocation space in block read buffer | real checked block I/O; no unsafe borrowed API |
| Scan 10k | 35.9% CPU in `pread`; 35.6% allocation in block decode and 34.6% in Scan result emission | remove candidate escape/redundant owned copies; retain materialized public ownership |
| frequent flush | 70% sampled CPU in filesystem syscalls; 65.8 ms sample | disk/fsync dominated; no durability weakening |
| medium compaction | 63.6% flat CPU in filesystem syscalls; block decode 68.1% allocation space | no merge rewrite; results environment-sensitive |
| Open/restart | 89% flat CPU in filesystem syscalls; directory reads/stat/open lead allocation | retain correctness-first recovery validation |
| mixed workload | 81.5% sampled CPU in filesystem syscalls; 253.7 µs total mutex delay | WAL sync dominates; lifecycle/mutex contention is not material |

The block profile was idle test harness and flush-worker waiting, not lock
contention. Goroutine counts remain bounded by the existing pipeline lifecycle.

## Startup and retained-WAL scaling

Three one-operation constrained samples produced these medians:

| Startup state | Median | B/op | allocs/op |
|---|---:|---:|---:|
| 1 live SSTable | 2.159 ms | 20,840 | 251 |
| 10 live SSTables | 2.962 ms | 109,680 | 1,553 |
| 100 live SSTables | 21.250 ms | 5,589,224 | 50,077 |
| replay 1,000 retained batches | 20.888 ms | 773,576 | 23,177 |
| replay 10,000 retained batches | 123.872 ms | 8,990,904 | 230,229 |
| replay 100,000 retained batches | 1.235 s | 95,607,752 | 2,300,695 |

Replay is approximately linear and the retained single WAL is a documented
lifecycle limitation, not a correctness blocker. A 500-table run was omitted
as low-value additional disk churn in the constrained environment. The cache
retains at most 32 readers by default; excess concurrent borrows are uncached,
so retained file descriptors do not grow with the live-table count.

## Optimization decision log

| Candidate | Measured? | Implemented? | Reason and result | Correctness result |
|---|---|---|---|---|
| frozen iterator | yes | yes | removes full snapshot; 100k allocation 36.1 MB → 0.8 MB | exact-order/ownership and pipeline tests pass |
| table cache | yes | yes | repeated Open dominated L0; 8-way allocation 19,864 B → 7,636 B | eager validation, lease/reclaim/Close tests pass |
| Scan owned transfer | yes | yes | candidate escapes/result copies visible; 100,822 → 71,152 allocs at 10k | model, ownership and scan tests pass |
| lazy validation | yes | no | cache solves open repetition without weakening Phase 1E trust | existing eager corruption contract retained |
| block cache | yes | no | checked block I/O is now hot, but cache could hide post-open corruption | rejected under keep-or-revert rule |
| Bloom filter | yes | yes | reserved v1 block; 8-way miss skips 8/8 reads, 0.75% measured false positives | zero-false-negative and corruption tests pass |
| group commit | yes | no | fsync dominates; Raft/data-WAL authority must be settled first | synchronous acknowledgement unchanged |
| merge rewrite | yes | no | allocations scale, but filesystem/block decode dominates profiled compaction | exact merge retained |
| Version refcounts | yes | no | measured mutex contention negligible | conservative Engine lock retained |
| WAL segmentation/GC | yes | no | 100k replay is visible but segmentation is a crash-safety subproject | active WAL deletion remains forbidden |

## Test-tier architecture

| Command | Purpose |
|---|---|
| `make test` | fast deterministic unit/integration and reference-model suite |
| `make check` | format, vet, lint, tidy, normal tests and diff check |
| `make race` | practical normal suite twice under the race detector; exhaustive campaigns remain separate |
| `make stress` | 100k MemTable/SSTable gates, 100-table compaction and the 50k-operation/120-restart Phase 1J campaign |
| `make crash` | subprocess write boundaries plus SSTable/Manifest/compaction publication crash cases |
| `make exhaustive` | every-byte WAL, SSTable and Manifest truncation campaigns |
| `make benchmark` | all microbenchmarks, five samples; honors `RIVETDB_BENCH_DIR` |
| `make certify-local` | check → race → stress → crash → exhaustive, failing on any tier |

PR CI runs static, normal tests on the floor and stable Go versions, selected
race and macOS tests. Scheduled/manual CI adds stress, crash and exhaustive
matrix jobs. No expensive campaign was deleted.

## Correctness certification

Phase 1J's fixed evidence remains: 50,000 operations (7,575 Put, 2,498
Delete, 22,499 Get and 17,428 Scan), 100 flushes, 24 compactions, 24 physical
reclamation passes and 120 restarts, with reference digest equality and zero
sequence/file-number reuse. Phase 1K reruns that known campaign plus a fresh
seed through `make stress`. The subprocess campaign covers `WRITE_ASSIGNED`,
`WAL_WRITTEN`, `WAL_DURABLE`, `MEMTABLE_APPLY_COMPLETED` and
`VISIBILITY_PUBLISHED`; the broader crash tier also covers SSTable publication,
Manifest durability/CURRENT switching and compaction installation.

The large randomized SSTable stress originally combined a fresh random value
stream with one fixed SHA-256 expectation. Phase 1K exposed that invalid test
construction after Bloom activation: distinct replayable seeds correctly
produce distinct tables. The fixed hash assertion was removed from that
randomized campaign; byte-format stability remains covered by the separate
deterministic table golden, while the 100,000-entry campaign still checks exact
decode equality and byte-identical output for each seed. Both observed failing
seeds were replayed successfully before the full stress rerun.

The final verbose source-freeze campaign used fixed seeds `101`, `9901` and
`8134472901` plus fresh seed `5138079162794469544`. It completed 50,000
operations (7,579 Put, 2,467 Delete, 22,363 Get and 17,591 Scan), 100 flushes,
24 compactions, 24 reclamation passes reclaiming 122 tables, and 120 restarts.
Its final reference-equal digest was
`901fc0d322a3dd1961971320ed81a6709c1a0400058260057d6afe54d1cd228f`;
sequence and file-number reuse were both zero.

Final command results and test inventory are recorded after the source freeze
at the end of this document.

## Raft-readiness audit

1. **Proposed flow:** client mutation → Raft proposal → committed command → a
   new deterministic replicated-state-machine apply API → local LSM mutation.
2. **Current `Engine.Put`:** must not be called independently as Raft apply. It
   assigns a replica-local sequence and writes the data WAL before applying.
3. **Sequence ownership:** the replicated command or applied log position must
   determine logical ordering identically on every replica. File numbers,
   compaction timing and SSTable layout may remain node-local because they do
   not change logical latest state.
4. **Durability authority:** Phase 2 must explicitly choose whether the Raft
   log is the replicated-write authority, whether the data WAL remains a local
   apply-recovery journal, or whether both coexist with distinct obligations.
   Accidental double logging is not accepted.
5. **Apply boundary:** it must accept an already ordered committed command,
   preserve idempotence across replay, and expose applied-index/visibility
   publication. It must not draw time or randomness or allocate a new logical
   order independently on each node.
6. **Snapshot/export:** Phase 3 needs either a consistent logical latest-state
   export plus applied index, or a pinned Version, its SSTables and required
   WAL frontier. Installation needs staging, validation, atomic authority
   switching and containing-directory fsync before publication.
7. **Phase 2 blocker:** no blocker to building Raft against the roadmap's
   injected in-memory state machine. The choices above are blockers to Phase 3
   integration and must be decided before Raft calls the Engine.

The Phase 3 read choice remains open. ReadIndex or a Raft-ordered read does not
need clock bounds; a leader lease would conflict with the current failure model
unless explicit clock-skew bounds are added. The Phase 4 shared-WAL question
also remains open; the current cache and benchmark-root options do not
foreclose one Engine/WAL serving multiple ranges.

## Freeze candidate and limitations

Stable Phase 2 inputs are the internal comparator and persistent encodings,
synchronous WAL-before-apply acknowledgement, assigned versus published
sequence distinction, atomic batches, immutable Version authority, replay
frontier, eager SSTable validation, latest-state Get/Scan semantics, explicit
Close and cache/reclamation leases. SSTable v1's format version and existing
encodings remain stable, but new writers now deliberately populate its
previously reserved optional Bloom-filter section. Older filterless v1 tables
remain readable and are tested.

Known limitations: single node; one ever-growing active WAL with physical GC
disabled; synchronous per-batch fsync and no group commit; no block cache;
eager startup validation; materialized Scan results; every
version and tombstone retained; experimental formats with no upgrade/backup
story; no MVCC transactions, snapshot transactions, serializable isolation,
Raft, replication, distributed transactions, range split/merge or migration,
adaptive rebalancing, server/client protocol, SQL or AI operator.

## Final gate record

Certification candidate: the commit containing this document, based on Phase
1J commit `2d84de1a0a39250d4b060e27b059b7f611b62fe4`. Actual final runs:

- Go 1.25.14 `make check`: **PASS** (gofmt, vet, lint with zero issues,
  tidy, normal tests, diff check).
- Go 1.25.14 `make race`: **PASS**, `-race -count=2`; the WAL package's
  final run completed in 231.746 seconds.
- Go 1.25.14 `make stress`: **PASS**, including the 50k-operation/120-restart
  Engine campaign and 100k-entry/100-table campaigns.
- Go 1.25.14 `make crash`: **PASS**; 10 selected top-level campaigns,
  including five subprocess write-boundary crashes.
- Go 1.25.14 `make exhaustive`: **PASS**; WAL and SSTable every-truncation
  offsets plus Manifest crash-at-every-offset.
- Go 1.25.14 `make certify-local`: **PASS** in check → race → stress → crash
  → exhaustive order. The crash selector was then broadened to include all
  publication/failure matrices and that expanded target also passed.
- Go 1.27.1 vet, normal tests and `-race -count=2`: **PASS**; the WAL race
  package completed in 231.521 seconds.
- Repository inventory: 247 top-level test functions and 56 benchmark
  functions; zero external modules, no `go.sum`; `go mod tidy` and
  `git diff --check` pass.

Representative disk latency, workload P50/P95/P99, concurrent-writer scaling,
and final physical write/space amplification remain explicitly
**deferred/environment-constrained**. That deferral does not weaken the
correctness certification or the CPU/allocation conclusions above.
