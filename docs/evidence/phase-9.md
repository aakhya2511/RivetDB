# Phase 9 certification evidence

## Environment

- Hardware: Apple M4, arm64
- OS: macOS 15.7.4 (24G517)
- Go: 1.25.14 darwin/arm64 (`GOTOOLCHAIN=local`)
- Repository: `/Users/aakhy/Documents/RivetDB`
- Test/build cache: `/private/tmp/rivet-phase9-go-cache`
- Test temporary root: `/private/tmp/rivet-phase9-tmp`
- Filesystem: APFS `/System/Volumes/Data`, 228 GiB total, 3.5 GiB
  available, 99% reported utilization at evidence capture
- Power: reports AC power while the battery reports discharging (59%)

The filesystem is nearly full and power is not demonstrably stable. Phase 9
publishes no disk-sensitive throughput claim. Correctness, race, randomized,
crash, CPU and allocation work remains valid; timings below are explicitly
non-disk planner baselines.

## Design and implementation evidence

- `PHASE 9 REBALANCING DESIGN GATE: PASS`
- Immutable canonical `RebalanceClusterSnapshot`, durable ReplicaID-keyed
  sampling, injected clock, exact integer EWMA and safe reset/regression logic.
- Pure deterministic planner with hard filters, start/recovery hysteresis,
  warm-up, durable cooldowns, projected sum-squared load potential and stable
  benefit/cost tie-breaking.
- MetaRange metadata version 3 persists the complete versioned policy,
  controller epoch, monotonic ActionID, bounded history, explanations,
  operation linkage, errors and cooldowns. Certified version 2 decodes with
  automatic rebalancing disabled until policy installation.
- Executor delegates only to Phase 8 `MoveReplica`, Phase 7 `SplitRange`, and
  the existing caught-up-voter leadership transfer.

## Targeted and randomized evidence

- `make rebalance-stress`: PASS; 100,000 modeled events, no duplicate target,
  failed target, unsafe source, or shuffled-input nondeterminism.
- `make rebalance-race`: PASS; full `internal/multiraft` and
  `internal/replicatedrange` suites under the race detector.
- `make rebalance-chaos`: PASS; automatic move/split/leadership transfer,
  restart reconciliation, hard target constraints and hysteresis/cooldown.
- `make rebalance-crash`: PASS; abrupt subprocess exit after action durability,
  migration submission, migration completion, split submission and split
  completion. Every restart converged to one SUCCEEDED action with at most one
  underlying operation.
- Automatic migration test commits a cross-range bank transfer and verifies
  historical MVCC visibility on the new replica.

## Planner baseline

Command: `go test ./internal/multiraft -run '^$' -bench
'^BenchmarkRebalancePlanner$' -benchmem -count=5`.

| Ranges | Observed ns/op | B/op | allocs/op |
|---:|---:|---:|---:|
| 10 | 8,865–13,520 | 21,968 | 134 |
| 100 | 70,858–112,027 | 196,048–196,049 | 1,127 |
| 1,000 | 855,653–1,266,622 | 2,041,306–2,041,311 | 11,031 |

These are constrained-environment CPU/allocation baselines, not representative
fsync, flush, compaction, migration-throughput or end-to-end latency numbers.

## Composed certification

`make certify-rebalance`: PASS on the exact final tree. This includes the
Phase 9 check/race/100k/chaos/crash tiers and the frozen Phase 8 migration,
Phase 7 split, Phase 6 transaction, Phase 5 MVCC, Phase 4 Multi-Raft, Phase 3
replicated-range, Phase 2 Raft and Phase 1 local-storage tiers.

Go 1.27.1 forward `go vet ./...`: PASS. Phase 10 readiness audit: deterministic
fault seams, controller stop/restart, authoritative reconciliation, stable
seeds and invariant IDs are present; network delay/throttling and long-running
history campaigns remain Phase 10 scope.
