# Phase 7 certification evidence

Date: 2026-09-11. Base commit: `c6e64280ff12f29545866f01fd4c4bee077eb8d6`.

## Result and design gate

`PHASE 7: PASS`

`PHASE 7 ONLINE SPLIT DESIGN GATE: PASS`

One statically bootstrapped MetaRange Raft group owns the dynamic catalog,
monotonic identifiers, split records and immutable lineage. A parent stays
authoritative through logical copy at S and original-timestamp delta replay.
After transaction drain, a short final write fence at F permits exact child
partition proof. One MetaRange CAS transfers authority to two independent
SHADOW child groups; committed proof activates them and permanently retires the
parent. Coordinator memory and directory discovery are never authority.

## Deterministic scenario evidence

- A three-node parent split produced new ranges 13 and 14, S=7 and F=8.
  Historical versions at HLC timestamps 720896000, 720896001 and 720896002,
  one historical scan, a tombstone, and an already-open snapshot all returned
  the same results after cutover. The retained old snapshot is explicitly a
  local historical handle into the physically retained parent, not routed user
  authority.
- One ordinary write committed at LEFT_IMAGE_DURABLE, replayed to the correct
  child, and was readable after cutover. Writes lost: 0. A transaction prepare
  attempted after the replicated begin fence was rejected.
- A prepared cross-range transaction with its outcome-home quorum unavailable
  blocked split drain. After quorum repair, Phase 6 epoch takeover durably
  aborted and resolved it; split recovery committed and terminal status stayed
  queryable.
- A transaction buffered against the pre-split generation aborted for retry
  after cutover. It was not silently retargeted.
- Lineage was exercised as `10 -> (13,14)` then `13 -> (15,16)`; resolving the
  original reference returned three ordered active leaves.
- A stale router sent to parent 10, received its committed redirect, refreshed
  catalog generation 8 once, and delivered to right child 14.
- Same-parent concurrent split initiation was rejected; different-parent
  initiations allocated distinct SplitIDs and four distinct child RangeIDs.
- An exact duplicate BeginSplit returned the existing SplitRecord without a
  second Raft mutation or any RangeID/SplitID allocation.
- Abort before F restored transaction admission and left catalog authority and
  generation unchanged.
- Parent, MetaRange and child leaders were each changed during one split. The
  protocol reached COMMITTED from durable replicated state.
- Full-cluster restart during COPYING resumed the split. A second restart after
  cutover retained active children and a RETIRED parent; no directory scan
  established authority.

## Crash matrix

`RIVETDB_SPLIT_CRASH=1 ... TestSplitSubprocessCrashMatrix` uses `os.Exit(73)`.
All 13 deterministic hooks passed on real FileStores and MVCC LSM directories:

| Abrupt exit | Authority immediately after exit | Recovery result |
|---|---|---|
| META_SPLIT_RECORD_DURABLE | old catalog, parent | takeover PREPARING; committed |
| PARENT_SPLIT_FENCE_ACTIVE | old catalog, parent | repeat fence/drain; committed |
| TXN_DRAIN_COMPLETE | old catalog, parent | establish S; committed |
| BOOTSTRAP_BARRIER_DURABLE | old catalog, parent | regenerate image at S; committed |
| LEFT_IMAGE_DURABLE | old catalog, parent | verify/rebuild remaining image; committed |
| RIGHT_IMAGE_DURABLE | old catalog, parent | verify readiness; committed |
| CHILD_QUORUM_READY | old catalog, parent | resume delta replay; committed |
| DELTA_REPLAY_PROGRESS | old catalog, parent | resume from durable frontiers; committed |
| FINAL_FENCE_DURABLE | old catalog, fenced parent | must finish forward; committed |
| CHILDREN_REPLAYED_THROUGH_F | old catalog, fenced parent | metadata CAS; committed |
| META_CUTOVER_DURABLE | new catalog, children | activate/retire forward; committed |
| CHILD_ACTIVATED | new catalog, children | repeat activation, retire parent; committed |
| PARENT_RETIRED | new catalog, children | terminal verification; committed |

Every subprocess recovered SplitID 1 and reported `writes_lost=0`. The hooks
cover the required conceptual windows: PREPARING, parent begin, post-drain,
S, each child image, child readiness, replay, immediate post-F, one/both
frontier progress (idempotent per child), metadata commit, partial local
observation/activation, and retirement. The separate unavailable-home test
exercises an interruption while drain is incomplete.

## Randomized and repeated campaigns

The normal five-node catalog reference campaign ran three 10,000-event seeds:
7001, 7002, and 8814833506225474754. Each attempted and committed 96 splits,
ending with 99 active ranges and zero aborts, gaps, overlaps, lineage cycles,
digest mismatches, or SPLIT invariant violations.

The opt-in heavy campaign used seed -7181725968542773413 for 100,000 events:
five nodes, three initial ranges, 99 final ranges, 96 attempted/committed, zero
aborted, gaps, overlaps, or cycles. This is a metadata/lineage model, so writes,
transactions, crashes, restarts, leader changes and stale redirects are zero in
that campaign; those dimensions are covered by the deterministic and
subprocess scenarios above. The real-filesystem tier completed 24 consecutive
disk-backed splits with zero digest mismatch. Restart coverage is supplied by
the mid/post-cutover restart and 13 subprocess cases, rather than by timing
each of those 24 operations.

## Engineering baselines

These are `CONSTRAINED-ENVIRONMENT BASELINE` measurements, not final or
representative disk-performance claims. The internal APFS volume was 228 GiB,
about 183 GiB used, 281 MiB available, and reported 100% utilization at final
capture. Benchmark/test data used `testing.TempDir` on that volume; no external
SSD was mounted. Hardware was Apple M4 arm64, macOS 15.7.4 (24G517), Go
1.25.14. Power reported AC while the battery was 46% and discharging.

One three-node, 100-version durable split measured:

| Stage | Time |
|---|---:|
| image export + left bootstrap | 5.199416s |
| remaining child bootstrap | 77.727583ms |
| initial replay/catch-up to final fence | 160.919042ms |
| final-fence replay/proof | 105.123875ms |
| metadata cutover | 82.991041ms |
| child activation + parent retirement | 80.967417ms |
| total split protocol | 6.080219542s |

CPU-only five-run ranges on the same machine were: 96-split metadata lookup
83.86–150.7 ns/op (63 B, 2 allocs); root-to-97-leaf lineage resolution
26.018–41.248 us/op (56,088 B, 408 allocs); canonical digest of 10,000 MVCC
tuples 460.328–536.613 us/op (32 B, 1 alloc, 670.87–782.05 MB/s). The split
baseline includes fsync/flush on a nearly full filesystem and must not be used
for external capacity or latency claims. `RIVETDB_BENCH_DIR` can place future
benchmark-owned data on an external local SSD.

## SPLIT invariants

- SPLIT-1/2/3/4/11/13/14: lifecycle admission, complete catalog validation,
  one MetaRange CAS, stale redirect, abort and restart scenarios.
- SPLIT-5/6/7/8/9/16: S/F image and partition proof, exact MVCC export,
  original timestamps, per-index replay advances, HLC floor, online write,
  history/scan/snapshot and every crash hook.
- SPLIT-10/17: replicated create/prepare fence, prepared drain blocker,
  takeover recovery, no-intent image and retained terminal archive.
- SPLIT-12: worker takeover epochs, leader changes, full restart and abrupt
  subprocess recovery without coordinator memory.
- SPLIT-15/18: exact descriptor replica inheritance plus monotonic durable
  RangeID/SplitID allocation in codec, concurrent and randomized tests.

## Certification commands

- `make certify-split`: PASS on its complete run. Its deduplicated composition ran `make check`,
  repository-wide race once, then every distinct Phase 7 through Phase 1
  stress, chaos, crash, corruption and exhaustive truncation tier once.
- `gofmt`, `go vet` with Go 1.25.14, golangci-lint v2.6.1 (`0 issues`), tidy,
  normal tests and diff check: PASS.
- `go vet ./...` with Go 1.27.1: PASS.
- Repository-wide `go test -race -count=1 ./...`: PASS; Multi-Raft 176.364s,
  replicated range 56.336s, engine 134.827s, WAL 105.090s, Raft 14.809s.
- Final-code Phase 7 stress 22.268s, chaos 17.775s, and crash 26.727s: PASS.
- Inherited transaction, MVCC, Multi-Raft, range, Raft and local storage
  certification tiers: PASS within `certify-split`, including 100k transaction
  and MVCC models and WAL/SSTable/Manifest every-offset checks.
- A final idempotency audit then added the duplicate-Begin verified no-op and
  its regression test. Post-change normal tests, lint, both vets, diff check,
  and focused race passed. A second whole-gate rerun passed `make check`, then
  APFS returned `ENOSPC` during the repository race at 200 MiB free; the failing
  seed was not promoted because the error was an explicit filesystem capacity
  failure, not a model mismatch. Only RivetDB-specific build caches were
  cleaned. The already-complete full gate plus the focused post-change delta
  gate form the certification evidence; no disk-performance inference is made.
- The final provenance tightening persisted child-specific image identity and
  made readiness verify every assigned replica. Online split, full restart,
  split chaos, all 13 abrupt crash stages, and focused race passed afterward;
  the complete final-code Multi-Raft and replicated-range suites passed in
  114.757s and 29.853s. The constrained baseline above is from that final code.

## Phase 8 migration-readiness audit

1. Logical MVCC image export, canonical digest, replicated bootstrap,
   provenance and parent-index catch-up can bootstrap a new replica.
2. MetaRange's versioned catalog, monotonic IDs, full-layout validation,
   generation CAS, immutable snapshots and recovery-first startup are reusable.
3. Phase 8 must add Raft joint consensus: add a non-voting learner, stream and
   catch it up, jointly promote it, transfer leadership if needed, jointly
   remove the old voter, and never remove before quorum-safe promotion.
4. RangeID remains the logical group. ReplicaID must remain a durable identity
   for one membership incarnation and a moved/new replica must receive a new,
   never-reused ReplicaID bound to its NodeID.
5. An old replica may be deleted only after committed metadata and committed
   Raft configuration exclude it, replacement catch-up is proven, no rollback
   references it, and the containing directory removal protocol is durable.
6. Migration must preserve transaction record/intent authority and allow
   prepare, decision and recovery through membership change without rewriting
   TxnID, RT, CT or participant RangeID.
7. Still needed: migration state machine/ADR/invariants, learners, portable
   snapshot streaming and bounds, resumable log catch-up, joint membership,
   ReplicaID allocation, cutover/rollback, deletion proof, hostile/crash tests,
   flow control and performance evidence. Phase 7 implements none of these.

## Scope audit

Not implemented: range merge, replica migration, automatic placement, adaptive
rebalancing, dynamic user-range membership, serializable transactions, SSI,
MVCC GC, tombstone GC, transaction-record GC, ReadIndex, linearizable reads,
SQL, or an AI operator.
