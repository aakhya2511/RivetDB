# Phase 5 evidence: replicated MVCC and range-local snapshots

Date: 2026-09-11. Certified Phase 4 parent:
`ad176e947d1adff3df9a8e35860fb85a82f81557`.

## Pre-implementation gate

The audit covered the internal-key comparator and candidate lookup, Engine
Get/Scan paths, replicated command/apply boundary, Manifest/VersionSet,
compaction, Range, Router, the Phase 1K evidence, and every prior phase design
and evidence note. ADR-0017 compares Raft indexes, range-local counters and a
globally comparable HLC. It chooses a range-scoped 48/16 HLC while preserving
the existing `user ascending, version descending, kind ascending` comparator.

`PHASE 5 MVCC DESIGN GATE: PASS`

A new persisted replicated-MVCC mode prevents Phase 3 directories, whose
internal sequence is a Raft index, from being silently reinterpreted as HLC
history. Raft index remains apply-order authority. The replicated timestamp is
logical visibility authority.

## Timestamp, storage and snapshot evidence

The timestamp is an unsigned 64-bit value: high 48 bits are Unix milliseconds
and low 16 bits are the logical counter. Canonical external encoding is
big-endian. Internal keys retain `BE64(^timestamp)`, so the explicit comparator
places higher timestamps first after user-key equality. Same-tick writes,
remote observation, the scripted physical sequence `1000,1001,995,996,1002`,
a 24-hour jump, logical overflow into a synthetic next millisecond, and maximum
timestamp exhaustion all pass without wrap or regression.

Leaders stamp canonical v2 PUT/DELETE command bytes before Raft proposal.
Followers apply exactly that value and observe it only for future leadership.
On open and before proposal, the range HLC observes the durable Manifest
maximum and every timestamp in the local durable Raft history, including
uncommitted entries. The leader-change test moves from a 5,000 ms clock to a
1,000 ms clock: timestamps remain `327680000..327680007`, then `327680008` on
the behind-clock leader and `327680009` after restart/election. Regressions are
zero. A durable uncommitted high timestamp on the successor is also skipped.

The exact `foo` matrix is A@100, B@200, DELETE@300 and C@400. Reads at
50/100/150/200/250/300/350/400/500 return respectively not-found, A, A, B, B,
not-found, not-found, C, C. A forced active/immutable/L0/L1 layout produces C,
not-found, B, A, not-found at 450/350/250/150/50. Binary-key reference checks
exercise the same candidate-seek boundary. Latest Get and Scan retain their
prior behavior.

The storage reference campaigns used seeds 501, 502 and 503, 10,000 events
each. Respectively they recorded:

| Seed | Put | Delete | GetAt | ScanAt | Flush | Compaction | Restart | Mismatch |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 501 | 2,599 | 448 | 4,930 | 965 | 1,058 | 44 | 4 | 0 |
| 502 | 2,527 | 429 | 5,022 | 1,006 | 1,016 | 33 | 4 | 0 |
| 503 | 2,502 | 426 | 5,039 | 1,011 | 1,022 | 39 | 4 | 0 |

Compaction preserves the exact internal-entry multiset and does not advance
the MVCC watermark. Historical GetAt/ScanAt matrices remain identical across
repeated flush, L0/L1 compaction, four reopen cycles per campaign, and physical
obsolete-table reclamation. No version or tombstone pruning exists.

A long-lived snapshot at B@T survives four newer writes, flush after every
write, compaction, and deletion of at least one obsolete table, and still
returns B. It tracks the oldest active timestamp, closes idempotently, releases
tracking, and returns `ErrSnapshotClosed` afterward. A closed range makes the
open handle return `ErrStopped`. One thousand simultaneous handles add no
goroutines and leave zero tracked handles after close. Snapshot handles are
process-local; their timestamps remain queryable after restart.

## Replication and randomized Multi-Raft evidence

Three replicas apply identical command timestamps and produce identical
logical `DigestAt` results at sampled historical timestamps. A request above a
replica's local applied MVCC maximum returns `ErrReplicaBehind`; an older
timestamp remains serviceable. Election no-ops advance Raft apply progress but
leave the MVCC watermark unchanged. A range whose physical clock is invalid
rejects timestamp generation with `ErrTimestampExhausted` while another range
on the same Multi-Raft node set continues to commit.

The real-filesystem normal Multi-Raft campaigns used five nodes, three ranges,
and seeds 511, 512 and 513, at 10,000 events each. Every run recorded 89 PUTs,
15 DELETEs, 9,758 GetAt calls, 47 ScanAt calls, 32 snapshots, 9 leader changes,
30 range partitions, 2 range-replica restarts, 3 node crashes/restarts and 8
flushes. Successful compactions were 5, 4 and 5. Historical digest mismatches
and MVCC invariant violations were zero.

The opt-in seed 517 campaign ran 100,000 events on five nodes/three ranges:
883 PUTs, 148 DELETEs, 97,608 GetAt calls, 465 ScanAt calls, 318 snapshots, 98
leader changes, 294 range partitions, 20 range restarts, 24 node
crashes/restarts, 78 flushes and 56 compactions. Digest mismatches and MVCC
invariant violations were zero.

These tests use real per-range Raft FileStores, Engines, WAL-free replicated
LSMs, Manifests, SSTable flush, compaction, reclamation and reopen. A separate
subprocess durably commits A, flushes it, commits an unflushed tombstone and B,
then exits with code 79 without Close. Restart under a regressed physical clock
recovers every historical read and assigns a strictly greater next timestamp.

## Engineering baseline and environment

These are `CONSTRAINED-ENVIRONMENT BASELINE` measurements, not production or
representative disk-performance claims. APFS `/dev/disk3s5` reported 228 GiB
capacity, 182 GiB used, 1.7 GiB available and 100% utilization. Benchmark data
used Go temporary directories on that internal volume; no external benchmark
volume was configured. Hardware was MacBook Air `Mac16,12`, Apple M4, 10 cores,
16 GiB; macOS 15.7.4 (24G517), Darwin arm64; Go 1.25.14 primary and Go 1.27.1
forward. Power was AC, battery 90% and charging.

Three 300 ms samples under Go 1.25.14 recorded:

| Operation | Observed range | Allocation |
|---|---:|---:|
| Get latest, 1,000-version hot key | 144.5–149.8 ns/op | 112 B, 5 allocs |
| GetAt recent, 1,000-version hot key | 146.2–150.6 ns/op | 112 B, 5 allocs |
| GetAt oldest, 1,000-version hot key | 200.4–206.5 ns/op | 112 B, 5 allocs |
| GetAt, depth 1 | 129.4–131.2 ns/op | 112 B, 5 allocs |
| GetAt, depth 10 | 141.3–148.9 ns/op | 112 B, 5 allocs |
| GetAt, depth 100 | 162.0–186.9 ns/op | 112 B, 5 allocs |
| GetAt, depth 1,000 | 165.0–281.2 ns/op | 112 B, 5 allocs |
| ScanAt, 1,000 keys | 77.906–82.704 us/op | 292,531 B, 3,029 allocs |
| snapshot create + close | 180.6–216.9 ns/op | 448 B, 5 allocs |

The version-depth benchmark deliberately remains in the active MemTable, so it
consults zero SSTable blocks and isolates candidate/comparator CPU cost. Disk
fsync, flush and compaction numbers are not offered as final performance
measurements under this filesystem pressure.

## Certification

`make certify-mvcc` passed formatting, Go 1.25 vet/lint/tidy/normal tests,
Phase 5 normal and twice-counted race tests, 100k stress, fixed/fresh chaos,
abrupt subprocess recovery, and every frozen Phase 4, Phase 3, Phase 2 and
Phase 1 certification tier. The inherited gate exposed and corrected a shared
scheduler completion-order bug: completion is now published only after pending
and statistics accounting, and the focused race test passed 20 consecutive
runs. Go 1.27 independently passed repository vet, normal tests and
twice-counted Phase 5 race tests. The exact commit is recorded in the Phase 5
completion report.

The certified boundary is replicated MVCC timestamp visibility and
range-local read-only snapshots. It is not a distributed-transaction,
cross-range atomic snapshot, linearizable-read, isolation-level, or MVCC-GC
claim.
