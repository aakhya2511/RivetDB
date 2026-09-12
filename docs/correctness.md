# Correctness Strategy

A distributed database is only as credible as the evidence that it is correct.
This document describes how RivetDB produces that evidence, and — equally
important — states the boundary of what is currently checked.

**Current scope:** Phases 0–9 exist: the complete local LSM, deterministic Raft,
durable replicated range, static Multi-Raft composition, and replicated MVCC
history with range-local snapshots, plus Snapshot Isolation transactions and
cross-range atomic commit. There is no network service, general distributed
read protocol, dynamic metadata, serializability or linearizability claim.
Sections marked *planned* are plans, not results.

---

## 1. The claim discipline

Every correctness claim in this repository must answer five questions:

1. Where is the implementation?
2. Where is the test?
3. What is the failure case that would break it?
4. What was measured?
5. What happens if the process crashes at the worst moment?

A claim that cannot answer all five is written as a plan or a limitation
instead. Specifically:

- RivetDB will not be described as *linearizable* unless a checker
  demonstrates it on recorded histories.
- RivetDB will not be described as *serializable* unless there is a test that
  fails when serializability is violated.
- Benchmark numbers are published with the hardware, the workload, the seed and
  the raw data, or they are not published.

The isolation level RivetDB supports will be stated as the level it is *tested*
at, together with an explicit table of which anomalies are prevented and which
are permitted.

---

## 2. Test layers

Each layer catches a class of bug the others cannot. The pyramid is designed so
that the cheapest layer that can find a bug is the one that does.

### 2.1 Unit and table-driven tests

For pure logic with enumerable cases: key encoding and decoding, Bloom filter
behaviour, Raft state transitions, the rebalancer's decision function. Critical
state machines get table-driven tests where each row is a
`(state, input) → (state, output)` transition, so the table doubles as the
specification.

What they cannot catch: anything involving interleaving, timing or crashes.

### 2.2 Property and model-based tests

The engine is run against a simple reference implementation — a sorted map for
the storage engine, a single-copy register for the replicated store — with a
seeded random operation sequence, and the two are compared after every
operation.

This is the highest-value test for the storage engine specifically, because it
explores states that hand-written cases do not: a flush landing between a write
and a read, a compaction dropping a tombstone whose older version is in a file
the test did not think about, an iterator opened just before a level merge.

### 2.3 Deterministic simulation

Time, randomness and message delivery are all injected
([`clock.Mock`](../internal/clock/mock.go),
[`testutil.RandFromSeed`](../internal/testutil/seed.go), and a simulated
transport). A whole multi-node scenario therefore runs in one process with no
real time and no real network, and is a pure function of its seed.

This matters more than it might appear. It means:

- A five-node partition scenario runs in milliseconds, so thousands of them fit
  in a CI run.
- A failure replays *exactly* — the same message order, the same timer firings,
  the same election. Debugging a consensus bug without this is a matter of
  guesswork.
- Rare interleavings can be searched for deliberately rather than waited for.

### 2.4 Crash and recovery tests

Durability is not testable by an ordinary happy-path test. The Phase 1B WAL
makes the crash point explicit by truncating a multi-block encoded segment at
every byte offset and asserting that recovery returns exactly the maximal
complete-record prefix. Its writer has narrow injected seams for short writes
and write/sync/close failures. Later engine phases extend this method to flush,
manifest and compaction operations.

The Phase 1C MemTable compares randomized operation sequences with a trusted
sorted-slice model, validates every skip-list level periodically, and runs a
separate 100,000-operation seeded structural gate. Concurrency tests mix 128
writers with readers, lower-bound seeks and iterator snapshots under the race
detector, and independently race all-or-nothing inserts against freeze.

The Phase 1D SSTable writer and Phase 1E reader now share one production-safe
decoder. Open streams every data block before trusting the resident sparse
index, proving restart structure, full-key boundaries, cross-block order,
metadata and typed block CRCs without retaining the whole file. Seek and
candidate property tests compare 30,000 queries each against a sorted-slice
model across six seeds; 3,000 half-open user-range queries compare exact entry
sequences. Tests rewrite semantic fields with valid checksums, mutate every byte
of a manageable table, run a separate seeded corruption campaign, truncate at
every offset, inject short reads and I/O failures, race concurrent operations
with Close, and explicitly validate a 100,000-entry/1,697-block table.

Phase 1F connects the WAL, MemTable and SSTable without claiming manifest
authority. Deterministic tests stop flushes to fill the bounded immutable
queue, prove cancellation occurs before another WAL append, then release the
worker and prove progress. A 64-writer accounting oracle matches every accepted
sequence/key to exactly one FIFO flush. Lifecycle transitions run through a
40,000-operation seeded model using three fixed seeds and one fresh seed,
including every invalid transition. Failure models cover WAL append/sync
classification and SSTable write, file-sync,
close, rename, directory-sync and reader-validation outcomes; failed or
ambiguous generations remain retained. Real-filesystem tests reopen every
flush through the production reader and replay the never-reclaimed WAL,
including the crash boundary after WAL durability and before MemTable apply.

Phase 1G establishes the metadata authority. Tests truncate a multi-edit
Manifest at every byte offset, reject middle corruption and malformed or
semantically invalid edits, and replay a deterministic 10,000-edit reference
model. CURRENT publication injects failure at temporary write/fsync/close,
rename and directory open/fsync/close. Filesystem integration proves that a
pre-Manifest table is an orphan, a post-Manifest-fsync table recovers as live,
missing/corrupt/mismatched live tables fail, old Manifests survive rewrite, and
WAL replay skips only complete batches at or below the durable inclusive
contiguous frontier. Concurrent readers run under the race detector against
immutable Versions.

Phase 1H treats compaction as exact structural replacement. Heap-merge,
overlap-closure, multi-output, oversized user-key group, stale-plan,
pre/post-Manifest crash, restart and 100-table stress tests compare complete
internal-entry multisets including tombstones and old versions. Inputs are
never physically deleted, and replay-frontier equality is checked across the
atomic replacement.

Phase 1I runs the integrated engine against a sorted latest-state reference
map across fixed and freshly logged seeds while interleaving writes, deletes,
flushes, compactions and reopen cycles. Targeted tests cover binary ranges,
overlapping L0 and L1 resolution, empty values, tombstone non-resurrection,
WAL replay without reappend, valid-orphan exclusion, missing/corrupt live-table
rejection and concurrent Get/Scan/write/flush/compaction under the race
detector. A Get or Scan is a sequence-bounded operation, not an MVCC snapshot
transaction, and no linearizability claim is inferred from these tests.

Phase 1J makes the visibility authority independently testable. Structured
write hooks stop one contiguous `Put(A), Put(B), Delete(C)` batch after
assignment, WAL write, WAL durability, apply start, apply completion and publication.
Concurrent Get and Scan operations see the complete old state at every
pre-publication barrier—even after the new internal entries are installed—and
the complete new state at publication. A child-process harness exits without
`Close` at the assignment/durability/apply/publication boundaries; the parent
reopens and checks absent non-durable versus present durable-but-unacknowledged
outcomes.

The always-on reclamation tests hold a Scan after it captures an old Version,
install a compaction replacement, and prove the exclusive maintenance pass
cannot unlink inputs until the Scan completes. Injected unlink failure leaves
the Version and reads unchanged and a retry succeeds. WAL maintenance validates
whole-batch/whole-file coverage and frontier straddles but leaves deletion
disabled for the single active append target. Corrupt unlisted final SSTables
are classified invalid and excluded; corrupt live tables fail Open.

The opt-in Phase 1J campaign uses three fixed seeds plus one freshly logged
seed for 50,000 modeled operations: 7,575 Put, 2,498 Delete, 22,499 Get and
17,428 Scan. It performs 100 flushes, 24 compactions, 24 reclamation passes and
120 close/open cycles. Every checkpoint compares explicit key results, a full
Scan and SHA-256 logical digest, validates live files and asserts monotonic
published/assigned sequence and file-number authority with zero reuse. This is
a latest-state model and crash/durability campaign, not a formal
linearizability or transactional history checker.

Phase 1K retains that model and adds exact equality between snapshot and direct
frozen MemTable traversal, caller-ownership mutation checks, bounded table-cache
reuse/eviction/Close tests, and a deterministic obsolete-reader lease through
compaction reclamation. Bloom testing inserts 10,000 deterministic user keys,
proves zero false negatives, measures absent keys rather than asserting zero
false positives, detects a filter bit flip by CRC, and rejects a
checksum-valid filter whose bits create false negatives during eager Open.
Existing post-open corruption tests remain unchanged; no block cache can hide
later file mutation.

Phase 2 adds a deterministic, single-owner Raft simulator with explicit logical
ticks, directed links, selected delivery, drop, delay, duplication, reordering,
partition, heal, crash, restart, proposal and snapshot operations. Every event
checks election safety, leader append-only, log matching, leader completeness,
state-machine safety, term/vote monotonicity, committed-prefix bounds and
snapshot/apply bounds. Targeted schedules cover one-, three-, four- and
five-node quorums, split votes, leader isolation, current-term-only commit,
conflict repair, full-cluster restart and snapshot catch-up. Fixed and fresh
seeds run at least 10,000 events per 3- and 5-node campaign; an opt-in tier runs
100,000 events. The independent Raft file store is truncated or corrupted at
every byte, rejects checksum-valid semantic corruption, ignores unpublished
temporary state, and injects failure at every publication step.

Expensive coverage is partitioned, not removed. `make test` is the normal
deterministic suite, `make race` repeats that practical suite under the race
detector, `make stress` enables large/reference campaigns, `make crash` runs
process/publication failures, and `make exhaustive` runs every-byte WAL,
SSTable and Manifest truncations. `make certify-local` runs all local-storage
tiers in order. Raft adds `raft-test`, `raft-race`, `raft-stress`, `raft-chaos`
and `raft-exhaustive`; `make certify-raft` runs those plus the unchanged local
certification. Scheduled/manual CI runs the heavy tiers separately from PR
latency.

Current crash coverage includes before-fsync, torn WAL/Raft-state publication,
flush, compaction and file-before-Manifest boundaries. Range-split and prepared-
transaction crashes remain explicit future gates for their respective phases.

### 2.5 Fault injection

*Implemented for the Phase 2 Raft simulator:* crash/restart, directed message
loss, selected delay/delivery, duplication, reordering and partitions. The
simulator records bounded deterministic traces. Disk throttling, process-level
network adapters and cross-subsystem faults remain Phase 10 work.

Phase 10 adds one seeded vocabulary for node/range crash and restart,
drop/delay/duplicate/reorder, directed range and node partitions, controller
and MetaRange outage, storage maintenance, split, migration, transactions and
leadership. Real short-write/fsync/corruption semantics remain in certified
component injectors rather than being approximated unsafely in the logical
model. See [chaos-testing.md](chaos-testing.md).

Asymmetric partitions get particular attention: they produce the stale-leader
scenarios that a symmetric partition test never reaches, and those are where
consensus implementations most often turn out to be wrong.

### 2.6 Chaos campaigns

Phase 10 composes a high-event deterministic global logical model with a
lower-count real durable five-node campaign. Cheap invariants run continuously
and expensive digests run periodically and after healing. Failures print the
seed and earliest failing prefix; seeds enter the tracked corpus only through
explicit promotion.

### 2.7 History-based consistency checking

Phase 10's bounded trace records each scheduled model operation and result with
logical time and authority identities:

```text
{client: 3, op: write, key: x, value: 1, invoked: t0, returned: t1}
{client: 7, op: read,  key: x,           invoked: t2, returned: t3, result: 1}
```

A recorded history is then checked against the consistency model RivetDB
claims. The scope of that checking will be stated exactly — which operations,
which keys, which model — and any property outside it will be listed as
unchecked rather than implied.

An ambiguous result (a client whose request timed out and may or may not have
been applied) is recorded as ambiguous, not as a failure and not as a success.
Discarding those cases would hide precisely the bugs worth finding.

---

## 3. Reproducibility

Reproducibility is a hard requirement, not a nicety. A randomized test that
cannot replay its failure produces a bug report nobody can act on.

The mechanism, implemented today:

- Every randomized test takes its seed from
  [`testutil.Seed`](../internal/testutil/seed.go), which logs it on every run.
- A failure prints the seed, the exact replay command, and an explicit
  `make promote-seed PACKAGE=... TEST=... SEED=...` command. It does not write
  repository files.
- After reproducing and fixing the failure, a developer runs that promotion
  command to append the seed to `testdata/seeds/<TestName>.seeds`.
- Those files are committed. `testutil.SeedCorpus` reads them so a test replays
  every historical failure before exploring new ground.
- `RIVETDB_SEED=<n> go test ...` (or `make seed SEED=<n> RUN=<pattern>`) forces
  a specific seed for a whole run.

The constraint this places on test code: a seeded `*rand.Rand` must not be
shared across goroutines. Sharing one makes the *sequence of draws* depend on
the scheduler, so the seed no longer determines the run. Each worker gets its
own source derived from the parent seed, or the schedule is generated up front.

---

## 4. Continuous integration

Per commit and pull request:

| Job | What it runs |
|---|---|
| static | `gofmt -l`, `go vet`, `golangci-lint`, `go mod tidy` check |
| test | `go test ./...` on the go.mod floor version and on `stable` |
| race | `go test -race -count=2 ./...` |
| cross-platform | `go test ./...` on macOS |

Race tests run with `-count=2` because a concurrency bug that reproduces
intermittently is worth two runs of a fast suite.

Nightly, and on a separate schedule: chaos campaigns and benchmarks, which are
too slow to gate a commit and too valuable to skip. They draw fresh seeds, so
they explore schedules that per-commit runs never reach.

`main` stays buildable. A phase gate that does not pass is not committed.

### Phase 3 replicated-range gate

`make certify-range` composes the independently certified Raft and LSM layers.
Targeted tests prove canonical command rejection, commit-before-apply recovery,
unflushed MemTable replay, orphan-table exclusion, atomic Manifest
table/frontier recovery, duplicate suppression, uncommitted-entry isolation,
fatal malformed apply, bounded proposal-waiter cleanup, immediate leader crash,
follower/full restart, different per-replica durable frontiers and intentionally
different physical layouts. Every replica also runs the raw Engine structural
validator.

Normal randomized integration runs 10,000 events per fixed/fresh campaign over
3- and 5-node groups with directed link changes, message drops, process-model
crashes/restarts, PUT/DELETE, flush and compaction. The opt-in tier runs 100,000
events. Unlike Phase 2 snapshot campaigns, integrated-range campaigns never
compact Raft history because LSM snapshot replacement is deferred. A separate
subprocess exits after a quorum-committed mutation and verifies reconstruction
from filesystem-backed Raft stores.

### Phase 4 Multi-Raft gate

`make certify-multiraft` composes the unchanged lower gates with descriptor,
catalog, node hosting, shared transport/scheduler and routed-mutation tests.
The catalog suite compares binary-search lookup with a linear reference over
random binary keys and rejects every truncation and single-byte corruption of
the canonical metadata format. Subprocesses exit at catalog publication
stages, during partial local-range bootstrap, and after several ranges commit.

A real-filesystem five-node/three-range layout exercises independent leaders,
range-specific quorum loss, node failure with different group effects,
range/node restart, different flush/compaction schedules, full-cluster restart,
per-range digest equality, and a global latest-state reference. A lightweight
five-range deterministic campaign checks every Raft group independently under
range/node partitions, crashes, restart, loss, duplication and reordering; the
opt-in tier runs 25 ranges for 100,000 events. The 100-group scheduler test
measures goroutine and heap growth and proves fixed round-robin tick service.

---

## 5. What is not tested

Stated plainly, because an unlisted gap reads as a claim:

- **Byzantine faults.** RivetDB assumes crash-stop. A node that lies is out of
  scope.
- **Silent disk corruption beyond checksum coverage.** Checksums detect
  corruption within a record or block. Corruption that a valid checksum would
  also accept — a whole stale-but-consistent file, for instance — is not
  detected.
- **Bounded clock skew.** Phase 5 HLC values use physical milliseconds but
  preserve monotonicity through replicated observation and logical increments;
  they make no bounded-skew or external-consistency claim. Phase 3 deliberately
  added no leader lease or distributed read protocol; any future lease design
  would require an explicit bounded-skew assumption and new tests.
- **Linearizable distributed reads.** Phase 3/4 `LocalGet`/`LocalScan` are replica
  inspection and may be stale. ReadIndex and lease reads are not tested because
  they are not implemented.
- **Integrated Raft/LSM snapshots.** Snapshot export and replace-state restore
  are deferred. Integrated ranges retain required history and do not expose log
  compaction.
- **Placement limits.** Phase 9 has automatic placement, splitting and leader
  balancing over the Phase 7/8 authorities. Its failure domain is NodeID; it
  has no topology/locality model, range merge, drain workflow, CPU/memory
  attribution, or performance-improvement claim.
- **Serializable or linearizable distributed reads.** Phase 6 certifies
  Snapshot Isolation transaction snapshots and atomic write commit. It does not
  add SSI, predicate validation, ReadIndex, leases, external consistency or a
  general linearizable distributed read API.
- **Adversarial input and authentication.** Input is bounds-checked and
  malformed data is rejected, but there is no threat model and no security
  testing.
- **Performance under sustained multi-day load.** Benchmarks are minutes, not
  days. Slow leaks and long-horizon compaction behaviour are not covered.
### Phase 5 MVCC gate

`make certify-mvcc` adds deterministic HLC regression/overflow tests,
timestamped-command validation, reference-model `GetAt`/`ScanAt`, active/
immutable/L0/L1 history, long-lived snapshot lifecycle, compaction/reclamation,
leader skew/change, durable restart, abrupt subprocess crash, Multi-Raft
historical digest, normal 10k-event fixed/fresh campaigns and an opt-in
100k-event campaign. It then runs every frozen Phase 4 and lower gate.

Certified visibility means the newest applied committed version at or below a
requested timestamp. It does not certify that a local replica is current with
the cluster; requests above its applied MVCC watermark return
`ErrReplicaBehind`. There is no transaction, isolation or linearizability
claim.

### Phase 6 transaction gate

`make certify-txn` adds canonical hostile transaction codecs, immutable RT and
read-your-writes, two/three/eleven-range one-CT commit, same-key races,
non-conflicting writes, deliberate write skew, stale coordinator fencing,
leader changes, range-specific failure isolation, committed-intent logical
interpretation, intent flush/compaction/restart, mixed-state full restart, and
real subprocess exit at seven durable 2PC stages. Fixed/fresh campaigns run
10,800 modeled operations and the opt-in stress tier runs 100,000. Exact reads,
scans, conflicts and committed histories are compared with a Snapshot Isolation
reference model. Every Phase 5 and lower gate remains a regression dependency.

### Phase 7 online split gate

`make certify-split` adds hostile metadata codecs, catalog/lineage model tests,
transaction fence and drain, exact logical MVCC partitioning, online delta
catch-up, final-fence proof, stale routing, independent child groups,
parent/meta/child leadership changes, full-cluster restart, and abrupt
subprocess exits at durable split boundaries. Fixed/fresh campaigns execute
10,000 catalog events per seed; an opt-in tier executes 100,000 events and
repeated disk-backed splits. Every Phase 6 and lower gate remains a dependency.

The certification establishes crash-safe ownership transfer and preservation
of acknowledged writes and historical MVCC state. It does not establish
zero-downtime writes at the final fence, automatic splitting, migration,
linearizable distributed reads, or serializable transactions.

### Phase 8 replica-migration gate

`make certify-migration` adds learner exclusion tests, canonical membership and
snapshot codecs, dual-majority quorum counterexamples, configuration restart
and snapshot persistence, bounded resumable snapshot staging, live follower
and leader replacement, transfer refusal for lagging voters, prepared intent
and TxnRecord preservation, historical digest equality, monotonic/repeated
ReplicaID allocation, stale-message rejection, durable retirement and real
deletion, plus abrupt subprocess recovery at partial-transfer, installed-
snapshot, joint, final-config, metadata-cutover and deletion boundaries. It
composes the exact Phase 7 and lower certification tiers.

### Phase 9 workload-aware rebalancing gate

`make certify-rebalance` adds injected-clock rates and integer EWMA tests,
counter-reset/clock-regression handling, tuple-keyed sampler identity, recovered
node and split-child warm-up, the exact alternating threshold sequence, spike
suppression, repeated convergence and 1,000-cycle no-churn checks. Fresh
validation rechecks execution-critical health, placement, capacity and
operation limits. Three 10,000-event stateful campaigns and one 100,000-event
campaign record moves, splits, leader transfers, failures, restarts, manual
operations, transactions, digests, cooldowns and stale plans. Real-filesystem
tests prove automatic execution, expected/actual score improvement, bank and
historical/latest digest preservation, and same-ID recovery after node failure.
Abrupt subprocess exits cover five controller/operation boundaries. The target
composes the exact Phase 8 through Phase 1 tiers.

### Phase 10 compositional chaos gate

`make certify-chaos` adds fixed/fresh 120,000-event normal campaigns, an exact
one-million-event heavy campaign, bounded replay traces, continuous logical
authority checks, targeted overlap pairs/triples, a real five-node durable
campaign and the transaction/split/migration/controller subprocess matrices.
The durable campaign uses actual FileStores, MVCC LSMs, Manifests, SSTables and
snapshot staging, then performs a full-cluster restart and logical comparison.
The target composes `make certify-rebalance`, hence every Phase 1--9 gate.
`make chaos-overnight` is the separate five-million-event scheduled tier.
