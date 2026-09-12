# Phase 10 certification evidence

## Environment and claim boundary

- Hardware: Apple M4, arm64
- OS: macOS 15.7.4 (24G517)
- Go floor: 1.25.14 darwin/arm64 (`GOTOOLCHAIN=local`)
- Repository: `/Users/aakhy/Documents/RivetDB`
- Build/test cache: `/private/tmp/rivet-phase10-*`
- Durable test roots: RivetDB-owned directories below the configured `TMPDIR`
- Filesystem: APFS, 228 GiB volume, 3.1 GiB free and 99% utilized before final gate
- Power: not used to make performance claims

Disk conditions qualify timing and are not a correctness waiver. Phase 10
publishes no representative fsync, flush, compaction or recovery-performance
number. Only explicitly named RivetDB cache/test roots were reclaimed; no user
files or unrelated data were removed.

## Design and model campaigns

The design gate and its fifteen decisions are in
[chaos-testing.md](../chaos-testing.md): `PHASE 10 CHAOS DESIGN GATE: PASS`.
`ChaosConfig` version 1 uses seven nodes, four initial ranges, at most 32 active
ranges, 128 modeled keys, 64 active transactions, a 4,096-event trace,
257-event digest period and deterministic seeded scheduling.

Normal campaign: fixed seeds 10001/10002 plus fresh replayable seed
7540322467939630497, 40,000 events each, 120,000 total:

```text
Put=10008 Delete=2502 GetAt=2502 ScanAt=2502
Txn begun=7503 committed=4696 aborted=2807 conflicts=305
splits started/completed/aborted=106/84/2415
migrations started/completed/aborted=1290/1247/1252
joint configs=2439 leader transfers=2499 automatic actions=2499
node crashes/restarts=2499/2499 range crashes/restarts=2499/2499
partitions/heals=4998/2502 drops/delays/duplicates/reorders=2499 each
MetaRange outages/recoveries=2499/2499 controller crashes/restarts=2499/2499
full-cluster restarts=2499 flushes/compactions/reclamations=2499 each
historical digest checks=5001 invariant checks=120000 expensive checks=471
stale messages/rejections=2499/2499 snapshots create/read=2502/2502
latest mismatches=0 historical mismatches=0 atomicity violations=0
SI violations=0 catalog violations=0 membership violations=0
identity resurrections=0 duplicate authoritative operations=0 lost acks=0
```

Heavy campaign: seed 100010, exactly 1,000,000 events:

```text
Put=83336 Delete=20834 GetAt=20834 ScanAt=20834
Txn begun=62501 committed=38997 aborted=23504 conflicts=2670
splits started/completed/aborted=36/28/20805
migrations started/completed/aborted=10624/10607/10226
joint configs=20755 leader transfers=20833 automatic actions=20833
node crashes/restarts=20833/20833 range crashes/restarts=20833/20833
partitions=41666 heals=20834; drop/delay/duplicate/reorder=20833 each
MetaRange outages/recoveries=20833/20833; controller crashes/restarts=20833 each
full-cluster restarts=20833; flush/compaction/reclamation=20833 each
historical digest checks=41667; invariant checks=1000000; expensive checks=3893
latest/historical mismatch=0; atomicity/SI/catalog/membership violations=0
retired resurrection=0; duplicate authority=0; lost acknowledged mutation=0
bank total=1000000; active ranges=32; open transactions after settle=0
```

The optimized heavy run completed in 40.18 seconds on this host. That is a
harness-engineering observation, not database performance evidence.

## Durable and crash campaigns

The real campaign uses five filesystem-backed nodes and three initial ranges.
It performs real MVCC writes and histories, a cross-range bank transaction
during learner migration, leader change, target flush/compaction, joint
consensus, source retirement/deletion, a fenced transaction during split,
another transaction during split image transfer, MetaRange leader failure,
range restart, full `Replica.Validate`, and a full-cluster reopen from committed
metadata. It ends with four active ranges, bank total 1,500, six historical
value checks, equal logical state, zero unexpected staging/temp files,
goroutines 2/2 and descriptor measurement unavailable under this sandbox.

The combined subprocess tier runs actual abrupt exits at every certified 2PC,
split, migration and controller interruption point. Inherited gates retain
real Raft-store corruption, Manifest/SSTable/WAL truncation, checksum mutation,
snapshot chunk corruption and storage publication failure coverage.

## Composition, convergence and resources

Targeted and scheduled coverage includes every pair/triple listed in the design
matrix. After healing, all nodes are marked healthy, pending transactions abort
authoritatively, incomplete model operations reconcile, one live voter is
selected per range and ten stable controller NOOP cycles occur before final
latest/historical/transaction digest checks. Trace length never exceeds 4,096;
terminal transaction state is capped at four times configured active capacity;
range and key growth are capped. Inspection does not alter workload counters.

Recovery latency is counted in logical recovery events (62,500 in the heavy
model); the real tier records completion only. Neither is an SLA. Expected
empty staging directories and retained terminal authority records are not
classified as leaks. Unexpected temp files, open operations and nonterminal
transactions after settlement are zero.

## Defects found and fixed

Two `SAFETY`-relevant restart defects were exposed by composing migration with
LSM maintenance and transactions:

1. A caught-up learner could flush beyond its retained bootstrap Raft snapshot;
   restart rejected the older snapshot even when its entire MVCC image was an
   exact subset of the farther-ahead durable LSM. Recovery now accepts only the
   byte-for-byte subset case, preserves the higher MVCC floor and replays the
   Raft suffix for metadata. Missing/mismatched snapshot content still fails.
2. Migration advances descriptor generation without changing RangeID or bounds.
   Restart could replay a durable transaction create from the prior generation
   and reject it as out-of-range. Same-RangeID older generations are now valid
   durable history; future generations remain rejected.

Harness defects found before evidence were retained: the first schedule did not
guarantee abort coverage and could oversubscribe `MaxRanges` through concurrent
pending splits. Both bounds are now asserted. No liveness or availability bug
was reclassified as safety. Low free space is `ENVIRONMENT` and caused no gate
failure in the recorded runs.

## Replay and Phase 11 readiness

Fresh normal seed replay:

```bash
RIVETDB_CHAOS_SEED=7540322467939630497 RIVETDB_CHAOS_EVENTS=40000 \
  go test -run TestChaosModelReplay ./internal/chaos
```

A 100,000-event replay profile showed `checkCheap` as the dominant model CPU
and allocation path before scratch reuse/incremental version checking; after
that evidence-driven harness optimization, the million-event run fell from
129.20 to 40.18 seconds. This is not a Phase 11 database optimization.

Phase 11 should measure, rather than infer, production CPU/allocation and user
latency. Existing evidence identifies synchronous multi-range 2PC round trips,
logical snapshot export/chunk/install/catch-up for migration, split image plus
delta replay, eager full validation, serialized synchronous paths and growing
WAL/MVCC history without GC as candidates. Controller planning is measurable
but currently secondary to state transfer. All disk-sensitive measurements
remain environment-constrained on this 99%-utilized APFS volume.

## Certification

The exact-tree certification command is `make certify-chaos`. It includes
static checks, normal tests, focused race, normal/heavy model, targeted
composition, real durable chaos, combined subprocess crashes and unchanged
Phase 9 through Phase 1 certification. The exact final commit and clean-tree
result are recorded after the final composed run.
