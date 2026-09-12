# Phase 11 performance engineering

## 1. Design gate

Phase 11 changes no database semantics. Measurements select work; the full
correctness stack decides whether a selected change may remain. The following
answers are binding before any performance-critical code is changed.

1. Honest workloads are local latest/historical reads and durable mutations;
   in-process 3/5-replica mutation; routing and many-range scheduling;
   read-only and 1/2/3/10-participant transactions; logical split/migration;
   controller collection/planning; and restart/replay at stated data sizes.
   Uniform, hot-key, read-heavy, write-heavy and conflict-heavy cases remain
   separate rather than being averaged into one headline.
2. Sampled wall-clock distributions are latency evidence. Completed operations
   divided by elapsed time (and payload bytes divided by time) are throughput
   evidence. `testing.B` reports CPU time and `B/op`/`allocs/op`; its averages
   are never relabeled percentiles.
3. Catalog lookup, codec/digest work, MemTable reads, scheduler/demux, planner,
   and in-memory Raft transitions are primarily CPU measurements. Mixed paths
   are classified from profiles, not their package name.
4. WAL/FileStore append, flush, compaction, durable open/replay, replicated
   apply and snapshot staging include local filesystem work and are disk-bound
   or mixed. Their fsync boundaries stay inside the timed operation.
5. Raft simulation, Multi-Raft routing, transactions, split and migration use
   the repository's in-process transport. They measure real state machines and
   local persistence but not NIC, RPC, TLS, kernel-network or datacenter cost.
6. On the recorded 99%-utilized internal APFS volume, all disk-sensitive
   numbers are `CONSTRAINED-ENVIRONMENT BASELINE`. Stable CPU/allocation
   microbenchmarks are publishable for this exact host. No representative
   local-disk result exists unless a documented unconstrained local SSD run is
   added.
7. User-facing priorities are Put/Delete, Get/GetAt/ScanAt, routed mutation and
   transaction commit. These precede background micro-optimisation.
8. Flush/compaction/open/replay, scheduler/demux, split/migration phases,
   metadata/lineage and controller planning are background or control-plane
   evidence and are labeled as such.
9. CPU profiles cover read-heavy, write-heavy, transaction, migration, split
   and mixed work. Allocation profiles cover ScanAt, transaction, snapshot,
   scheduler and planner. Mutex/block profiles cover mixed replicated,
   transaction-heavy and many-range work. Top flat/cumulative sites, rather
   than intuition, nominate candidates.
10. A candidate is justified only by a repeatable baseline/profile hotspot and
    five-sample A/B evidence with allocations. At most five broad, low- or
    medium-risk changes may remain.
11. One-phase commit, Raft group commit, lock redesign, lazy SSTable validation,
    raw-SSTable snapshot sharing and unsafe zero-copy ownership are presumed too
    correctness-sensitive unless an overwhelming measured benefit and a new
    proof overturn that presumption. Phase 11 does not add those semantics.
12. README-safe claims name the host, exact workload boundary and whether the
    result is CPU-only, constrained local disk, or in-process replication.
    Production, real-network, cloud-scale and cross-database claims are unsafe.

`PHASE 11 PERFORMANCE DESIGN GATE: PASS`

## 2. Environment recorded before measurement

Recorded 2026-09-12 before Phase 11 performance code changes:

| Field | Value |
|---|---|
| Starting commit | `2f044efafc9a38c9ec54a988d3e1942fcb2285f8` |
| Hardware | MacBook Air `Mac16,12`, Apple M4, arm64 |
| CPU | 10 cores: 4 performance + 6 efficiency |
| Memory | 16 GiB |
| OS | macOS 15.7.4, build 24G517; Darwin 24.6.0 |
| Filesystem | APFS Data, `/dev/disk3s5`, 228 GiB capacity, 178 GiB used, 3.3 GiB free, reported 99% utilization |
| Power | AC power; battery 62%, charging |
| Go floor | Go 1.25.14 darwin/arm64, `GOTOOLCHAIN=local` |
| Forward check | Go 1.27.1 darwin/arm64 |
| GOMAXPROCS | unset; runtime default 10 |
| Benchmark root | `RIVETDB_BENCH_DIR=/private/tmp/rivet-phase11-bench` |
| Cache roots | RivetDB-owned `/private/tmp/rivet-phase11-{gocache,gomodcache,tmp}` |
| External local SSD | none detected |
| Build flags | default compiler; benchmark flags `-run '^$' -bench <fixed-regexp> -benchmem -count=5`; migration/split whole-operation cases use `-benchtime=1x` |
| Classification | `CONSTRAINED-ENVIRONMENT` for every disk-sensitive result |

Only bounded RivetDB-owned roots may be reclaimed. The suite must not inspect,
delete or fill unrelated user storage. Power and filesystem state are recorded
again with final evidence.

## 3. Benchmark contract

The stable matrix is exposed as `make benchmark-storage`,
`benchmark-distributed`, `benchmark-txn`, `benchmark-control`, and
`benchmark-profile`; `make benchmark` composes the benchmark groups. Each
benchmark documents setup excluded from timing and reports allocations. A
bounded latency sampler records independent operation durations and computes
P50/P95/P99 directly; it does not infer distributions from `testing.B`.

Published results use at least five samples and report a median plus range.
Cold open/recovery is kept separate from warm reads. Whole-operation split and
migration results report milestones as well as totals. Scaling axes are version
depth, range count, participant count, writer concurrency and logical
snapshot/data size. Setup counts, keys, versions, ranges, bytes, table counts,
durability mode and excluded bootstrap work accompany results.

## 4. Candidate protocol and claim boundary

Every candidate decision records baseline, profile, hypothesis, implementation,
A/B time, allocations, focused correctness, risk and retain/reject/defer. The
audit explicitly covers serial 2PC prepare/resolution, logical snapshot copies
and chunk sizes, eager open validation, table/Bloom behavior, ScanAt assembly,
intentional serialization, Raft group commit, history growth and scheduler/
transport contention.

Logical snapshot portability, eager corruption detection, transaction-record
authority, participant idempotency, Raft commit order, standalone WAL
durability and immutable return-value ownership are frozen correctness
boundaries. A performance result cannot override them.

## 5. Workload boundaries

- Local operations include their configured standalone WAL durability. Flush,
  compaction and restart include local filesystem publication and validation.
- Replicated mutation includes in-process delivery, three or five local Raft
  FileStore publications and WAL-free LSM apply; election/bootstrap is excluded.
- Transaction commit includes Begin barriers, PENDING, participant prepares,
  terminal decision and resolution. Cluster creation and elections are excluded.
- Split/migration totals include their certified logical protocol after a
  bootstrapped/elected fixture. Individual phase metrics are elapsed milestones.
- Planner benchmarks consume a prebuilt immutable snapshot unless explicitly
  labeled collection; neither represents controller sleep/cooldown time.

No benchmark here measures a real RPC transport, multi-host clock/network
effects, production availability, linearizable reads or serializable isolation.

## 6. Freeze decisions

Three changes are retained: skip table selection when immutable Version
metadata proves a point read has no tables; consume read-only bytes already
owned by a MemTable iterator snapshot during scan assembly; and compute planner
projected potential without per-candidate node/map materialization. The complete
baseline, profile, hypothesis, five-sample A/B, allocation, correctness and
tradeoff record is [evidence/phase-11.md](evidence/phase-11.md).

One owned Raft decode arena was reverted after a time regression despite its
allocation win. Parallel 2PC, one-phase commit, Raft group commit, larger
snapshot chunks, lazy validation, block caching, raw physical snapshots and
lock redesign are rejected or deferred. The measured evidence did not justify
their correctness or operational risk on this constrained host.

`PERFORMANCE FREEZE CANDIDATE` means no further performance code changes occur
unless the exact final correctness gate finds a regression.
