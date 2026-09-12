# Phase 11 performance certification evidence

## Environment and claim boundary

The pre-measurement record is in [performance.md](../performance.md). The host
was a 16 GiB MacBook Air (Mac16,12), Apple M4 arm64, ten cores, macOS 15.7.4
(24G517), Go 1.25.14 with the runtime-default GOMAXPROCS=10. Repository and
benchmark root were on `/dev/disk3s5`, APFS Data, 228 GiB total. Free space was
3.3 GiB (99% reported utilization) before measurement and fell to 1.6 GiB
during final work. No external physical SSD was detected. Power was AC,
charging at 62% initially.

Benchmark data used `RIVETDB_BENCH_DIR=/private/tmp/rivet-phase11-bench`; Go
cache and temporary roots were `/private/tmp/rivet-phase11-*`. Every root is
RivetDB-owned and bounded. No unrelated file was inspected or deleted.

Accordingly, all fsync, FileStore, flush, compaction, open/replay, transaction,
split and migration timings are `CONSTRAINED-ENVIRONMENT BASELINE`. CPU-only
and allocation microbenchmarks are valid for this exact host. Replicated paths
use an in-process transport and are not real-network or production results.
The compact machine-readable summary is
[phase-11-benchmarks.csv](phase-11-benchmarks.csv).

## Method

Published key results use five samples and report median and range. Targeted
A/B comparisons ran the exact Phase 10 tree from an owned `git archive` beside
the candidate under matched environment and benchtime. Direct operation
samples—not `testing.B` averages—produce P50/P95/P99. The samples are bounded
at 4,096 observations and explicitly warmed where relevant. Whole-operation
transaction distributions use five observations per sample, so P95 and P99
coincide; they remain local constrained evidence.

`make benchmark` passed with five one-iteration samples across the complete
matrix; longer targeted runs provide stable headline CPU/latency numbers.
`make benchmark-profile` captured CPU, allocation, mutex and block profiles in
an uncommitted RivetDB-owned profile root.

## Baseline profiles and decisions

| Candidate | Evidence | Risk | Decision |
|---|---|---:|---|
| Empty-Version point-read fast path | `getMVCCAt` 67% cumulative; `cloneTables`, growth and allocation visible despite no SSTables | low | retain |
| MemTable scan borrowed snapshot values | ScanAt 365,474 B/3,030 allocs; 96.6% of allocation objects in `bytes.Clone` | medium ownership | retain with ownership regression |
| Allocation-free projected planner score | 1,000-range plan 2.04 MB/11,035 allocs; projected slices/maps dominate | low, pure math | retain with reference equivalence test |
| One owned Raft decode arena | 100,001→2 allocs but median decode time regressed about 6% | medium ownership | reject; code removed |
| Parallel prepare/resolution | 3N+3 Raft commits and serial 2N participant rounds scale visibly | high recovery/scheduler concurrency | defer |
| One-phase commit | would remove protocol work for one range | very high new authority path | reject |
| 256 KiB migration chunks | 4 MiB constrained run: median 260 ms at 64 KiB vs 76 ms at 256 KiB | medium retry/tail tradeoff; disk unrepresentative | defer; keep 64 KiB |
| Raft group commit | 1–32 writers remain about 306–327 ops/s and syscalls dominate | very high durability/ack ordering | defer pending representative disk evidence |
| Lazy SSTable validation | 1/10/100/1,000-table cold open median 0.345/0.482/4.23/305.5 ms | high corruption-boundary risk | reject; eager validation retained |
| Block cache | repeated checked reads remain visible | high post-open corruption semantics | defer |
| Table-cache/Bloom retune | warm opens approach zero; eight-table miss skips 8/8; Phase 1K FPR 0.75%, 1.2516 B/key | low benefit | retain current settings |
| Lock rewrite | mutex samples show no dominant application lock | high complexity | reject |
| Raw-SSTable/unsafe zero-copy snapshots | logical portability is load-bearing | high ownership/corruption risk | reject |

## Retained optimisation A/B

1. Empty-Version `Get`: matched median 193.8 ns (180.9–208.2) to 150.8 ns
   (139.4–171.7), 22.2% faster. Allocations remain 120 B/6. The branch only
   skips table selection when immutable Version metadata proves there are no
   tables; MemTable and result-copy semantics are unchanged.
2. ScanAt snapshot consumption: 365,474 B/3,030 allocations to 357,474 B/2,030
   allocations (2.2% bytes, 33.0% objects). Matched timing medians 231.2 vs
   237.0 us overlap broadly, so no latency improvement is claimed. The engine
   borrows only bytes already owned by its iterator snapshot; a test mutates a
   returned result and proves later reads remain unchanged.
3. Planner candidate scoring: at 1,000 ranges median 1.921 ms
   (1.725–2.983) to 1.620 ms (1.256–2.106), 15.7% faster; 2,041,712→1,433,707
   B (29.8%) and 11,035→6,035 allocations (45.3%). A materialized reference
   test proves identical integer potential for moves and leader transfers;
   the existing deterministic plan/model suite passed.

The final read profile no longer contains `manifest.cloneTables` in its top
sites; `getMVCCAt`, MemTable lower-bound/comparator work, result ownership and
runtime allocation now dominate. Planner canonicalization/sorting remains its
largest allocation family. ScanAt remains copy-heavy because returned keys and
values must not expose mutable engine memory.

## Representative matrix results

There are no representative disk results on this host. Selected exact-host
medians follow; time ranges and allocations are in the CSV.

| Path | Median | Boundary |
|---|---:|---|
| active-MemTable Get | 150.8 ns | CPU, returned value copy included |
| old GetAt, 1,000 versions | 354.2 ns | CPU, in-memory skip-list |
| ScanAt, 1,000 keys | 138.7 us | CPU/allocation, full materialization |
| durable Put | 3.156 ms / 317 ops/s | constrained local WAL fsync |
| durable Delete | 3.092 ms / 323 ops/s | constrained local WAL fsync |
| 1,000-entry flush | 10.93 ms | constrained publication/fsync |
| four-table compaction | 14.15 ms | constrained read/write/publication |
| 3-node durable mutation | 34.99 ms / 28.6 ops/s | constrained in-process, three FileStores |
| 5-node durable mutation | 56.93 ms / 17.6 ops/s | constrained in-process, five FileStores |
| routed mutation | 44.67 ms / 22.4 ops/s | constrained in-process, routing included |
| 2-participant transaction | 288.4 ms / 3.47 txns/s | constrained in-process complete 2PC |
| split total | 954.1 ms | constrained, empty logical image fixture |
| migration total | 807.2 ms | constrained, 156-byte snapshot fixture |
| 1,000-range controller plan | 711.8 us | CPU-only final run |
| 1,000-range collect | 11.16 ms | in-process empty-range telemetry |

Direct sampled distributions use medians across the five runs: durable Put
P50/P95/P99 3.00/3.93/7.78 ms; active Get 0.125/0.334/0.417 us; old GetAt
0.250/0.542/0.792 us; two-participant commit 290.0/298.0/298.0 ms; routed
mutation 48.0/54.2/54.2 ms. The latter two have only five observations per run.

The local mixed benchmark is 70% Get, 20% Put and 10% Delete; its approximately
1.12 ms/op profile is syscall-dominated because 30% of operations are durable.
No real-network throughput is measured.

## Scaling and protocol audit

| Axis | Observed medians |
|---|---|
| versions/key GetAt | 1: 210.8 ns; 10: 346.8 ns; 100: 298.8 ns; 1,000: 255.0 ns; 10,000: 375.8 ns |
| catalog ranges | 1: 25.3 ns; 100: 64.4 ns; 1,000: 69.4 ns; hard maximum 4,096: 68.7 ns |
| transaction participants | read-only 23.21 ms; 1: 192.6 ms; 2: 288.4 ms; 3: 402.1 ms; 10: 1.293 s |
| durable writer concurrency | 1/2/4/8/16/32: 314.3/318.8/320.5/323.6/322.6/296.6 ops/s-equivalent |
| logical image digest | 1k/10k/100k versions: 45.7 us/373.0 us/3.57 ms, roughly 0.79–1.01 GB/s |
| 4 MiB staged snapshot chunks | 32/64/128/256 KiB: 510.8/260.0/133.9/75.7 ms constrained medians |
| WAL replay | 1k/10k/100k batches: 6.65/36.98/346.6 ms constrained |
| cold SSTable open | 1/10/100/1,000 files: 0.345/0.482/4.23/305.5 ms constrained |

The catalog's certified hard limit is 4,096 ranges, so a 10,000-range catalog
benchmark is invalid and was not fabricated. Scheduler ticks were exercised at
10/100/1,000 hosted groups; the fixture creates one bounded pipeline worker per
hosted range (measured deltas: 10/100/1,000 goroutines). One-iteration tick
costs were 0.163/1.129/7.726 ms with 920/7,496/72,392 B, but timing depends on
whether a logical tick crosses an election/
persistence boundary, so these results are profile inputs rather than a clean
scaling headline. Fairness remains the certified queue result: after 51 hot
items, a cold item is selected second; no scheduler QoS change was justified.

For N written participants, the existing complete transaction path performs
one Begin timestamp/barrier commit, N-1 additional serving barriers, one CT
assignment, one PENDING record, N prepares, one decision and N resolutions:
`3N+3` Raft commits total (6/9/12/33 at N=1/2/3/10) and `2N` serial participant
rounds. Each FileStore Save retains file-fsync then rename then directory-fsync;
one Raft commit is not falsely equated with one fsync because message handling
may save several replica states. This measured scaling supports documenting
serial 2PC, but not adding concurrency to the synchronous scheduler in Phase 11.

## Profiles, resources and limitations

Write/mixed, transaction, migration and split CPU samples are dominated by
`syscall.syscall`/`runtime.fcntl`, consistent with local durable publication.
Read CPU is `getMVCCAt`, MemTable lower-bound/internal comparison, ownership
copies and runtime allocation. Migration allocation is mostly repeated catalog
construction and gzip writer setup; transaction allocation is mostly byte and
membership clones. Many-range allocation is Raft status-map cloning plus
planner canonicalization. Mutex profiles show only small runtime/Manifest
events. Block profiles are primarily expected `select`, channel receive and
shutdown `WaitGroup` waits—not foreground lock contention.

No MVCC, tombstone or transaction-record GC means disk/state growth is
unbounded. Retained WAL/MVCC histories increase restart and physical size;
100,000 standalone WAL records replay in a constrained median 346.6 ms and
allocate about 95.6 MB. Logical snapshot staging performs an fsync per chunk;
large transfers remain expensive. Range merge, linearizable reads, real RPC,
sustained multi-day load, production hardware and representative local SSD
latency remain unmeasured.

## Phase 12 advisory boundary

An optional AI operator may inspect immutable telemetry snapshots, policy,
plans, action/cooldown history, range/node status and benchmark/environment
evidence. It may recommend deterministic move, split or leader-transfer inputs.
Every action must still pass the existing fresh-state validator and certified
Phase 7/8 protocol; AI cannot bypass authority. An explanation must include
snapshot/policy generations, measured metrics, violated threshold, candidate
score/cost/benefit, constraints, confidence/limitations and validator outcome.
The clean integration is an advisory consumer of canonical snapshots producing
ordinary proposed actions. It can be absent or disabled with zero effect on
correctness, telemetry, planning or control.

## Gate

`make benchmark` and `make benchmark-profile` passed before the final full
correctness gate. Focused storage/planner ownership and equivalence tests pass.
The exact final `make certify-chaos`, inherited gates, final environment, commit
and clean-tree state are recorded after the no-more-changes freeze run.
