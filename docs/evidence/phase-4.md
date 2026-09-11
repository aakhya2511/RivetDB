# Phase 4 evidence: Multi-Raft and static range routing

Date: 2026-09-11. Certified Phase 3 parent:
`96426135861644ced8c956d0046a4cf19cac3688`.

## Pre-implementation gate

The starting worktree was clean and Phase 3's final composite certification was
recorded PASS. Audit found no process-global Raft, storage Engine, Manifest,
applied frontier, waiter map, fatal state, timer, or transport goroutine.
Phase 3's synchronous instance-scoped boundary can host multiple replicas.

`PHASE 4 MULTI-RAFT DESIGN GATE: PASS`

ADR-0016 chooses per-range consensus/storage behind shared bounded node-level
transport and fair scheduling. The authoritative static catalog is persisted,
checksummed, generation-bearing, and independent of range directories.

## Implementation and certification

`internal/multiraft` implements explicit descriptor bounds, immutable catalog
snapshots, deterministic binary catalog persistence, per-node range registries,
generation-bearing authenticated message envelopes, one bounded fair
transport, one round-robin scheduler, static node bootstrap/degraded status,
range-local restart, and bounded routed PUT/DELETE with per-range leader hints.
Phase 3 admission and committed apply both enforce descriptor ownership.

The catalog decoder rejected every truncation and every single-byte corruption
of a three-range catalog. Publication uses temp contents fsync, close, rename,
and directory fsync. Subprocesses exited at `TEMP_DURABLE`, `RENAMED`, and
`DIRECTORY_DURABLE`; every restart recovered or deterministically completed the
same catalog. Another subprocess exited after the first of three local ranges
opened but before the completion marker, then restart created all and only the
assigned replicas. Same-generation completion-marker corruption fails startup.

The real-filesystem topology is five nodes and three RF=3 ranges:

```text
R10 [-inf,g)  N1,N2,N3  leader N1
R11 [g,p)     N2,N3,N4  leader N3
R12 [p,+inf)  N1,N3,N5  leader N5
```

Ninety routed mutations converged per range with no ownership or digest
mismatch. Every range reached applied index 31. Representative live-table
counts were R10 `[30,15,10]`, R11 `[15,10,7]`, and R12 `[29,9,5]`, proving
physical divergence. Range-specific partitioning made R10 quorumless while R11 committed.
A separate placement test crashed N1 from R10=N1/N2/N3 and R11=N1/N4/N5;
both retained quorum, then loss of N2 prevented an R10 election while R11
continued. Twelve disk-backed cycles performed 36 routed mutations, 24 flushes,
18 successful range-local compactions, and 12 whole-node reopen operations.
Full cluster restart recovered the catalog and all nine local replicas; each
range's three replicas reached applied index 12 after 10 pre-crash commands and
the two election no-ops. An abrupt
subprocess committed one key in every range and recovered all three without a
graceful Node or Engine close.

The recorded disk-backed randomized seed `5494060941595339824` ran 400 events:
100 accepted PUT/DELETE proposals, 39 range partitions, 41 node partitions, 50
range-replica restarts, 44 node restarts, 48 flushes, 35 successful compactions,
zero invariant violations, zero digest mismatches, and exact global-reference
equality. The seed is promoted.

The lightweight normal campaign runs fixed seeds `401`, `402`, promoted seeds,
and one fresh seed over five nodes, five ranges, and 10,000 events each. One
recorded fresh campaign (`-2203619777110060550`) plus the fixed campaigns
totaled 234 accepted proposals, 790 range partitions, 756 node partitions, 424
range-replica crashes, 758 node crashes, 1,550 restarts, and zero invariant or
digest failures. The heavy 25-range/100,000-event run used seed
`-1543413411421534143`: 104 accepted proposals, 2,651 range partitions, 2,562
node partitions, 1,334 range-replica crashes, 2,566 node crashes, 21,223
restarts, and zero invariant or digest failures.

The 100-group disk-backed hosting test uses a single Node/Transport/Scheduler,
performs 1,000 round-robin ticks, and observed 2 baseline versus 102 hosted
goroutines: exactly one existing bounded LSM flush worker per range and no
timer/transport/deterministic-scheduler goroutine. The physical Runtime uses a
fixed four-worker pool regardless of group count. A deterministic blocked-hook
test queued 51 hot-range items; a cold-range item completed on the second
worker before the hot hook was released, and all 52 items then drained.

## Engineering baseline and environment

These are `CONSTRAINED-ENVIRONMENT BASELINE` measurements, not production
claims. APFS `/dev/disk3s5` had 228 GiB capacity, 4.7 GiB available and 98%
utilization. Benchmark roots were Go temporary directories on that internal
volume. Hardware: MacBook Air `Mac16,12`, Apple M4, 10 cores, 16 GiB; macOS
15.7.4 (24G517), Darwin arm64; Go 1.25.14 primary and 1.27.1 forward. Power was
stable AC with battery charged.

Three samples each recorded:

| Operation | Observed range | Allocation |
|---|---:|---:|
| catalog lookup, 1 range | 35.37–46.47 ns/op | 16 B, 1 alloc |
| catalog lookup, 100 ranges | 80.81–95.83 ns/op | 32 B, 3 allocs |
| catalog lookup, 1,000 ranges | 113.0–132.8 ns/op | 32 B, 3 allocs |
| shared envelope enqueue/dequeue | 95.63–102.3 ns/op | 0 B, 0 allocs |
| logical tick, 100 hosted groups | 153.959–156.753 us/op | ~1,147 B, 14 allocs |
| routed durable mutation, 3-node group | 46.551–50.018 ms/op | ~61,949 B, 835 allocs |

The routed mutation and scheduler samples touch disk; filesystem pressure
qualifies them and no distributed latency/throughput claim is made.

## Certification

`make certify-multiraft` passed the Phase 4 normal, race, stress, chaos and
subprocess crash tiers and the inherited `certify-range`, `certify-raft`, and
`certify-local` gates. The final Phase 4 package passed `-race -count=2` under
both Go 1.25.14 and Go 1.27.1; repository-wide Go 1.27.1 vet and normal tests
also passed. The primary gate includes formatting, Go 1.25 vet,
golangci-lint, tidy verification and `git diff --check`.

A final fresh five-node/five-range campaign used seed
`-972856019196534012`; together with fixed seeds 401 and 402 it ran 30,000
events, 218 accepted proposals, 799 range partitions, 801 node partitions,
428 range crashes, 762 node crashes, 1,542 restarts, 193 duplicated messages,
212 dropped messages, zero invariant violations and zero digest mismatches.
A fresh disk-backed run used seed `-7469589995887258700`: 400 events, 126
accepted proposals, 41 range partitions, 38 node partitions, 33 range crashes,
49 node crashes, 82 restarts, 30 flushes, 32 successful compactions, zero
invariant violations and zero digest mismatches. These fresh runs supplement,
rather than replace, the promoted/replayable evidence above.
