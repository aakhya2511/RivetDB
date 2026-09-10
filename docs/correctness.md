# Correctness Strategy

A distributed database is only as credible as the evidence that it is correct.
This document describes how RivetDB produces that evidence, and — equally
important — states the boundary of what is currently checked.

**Current scope:** Phase 0, the pre-Phase-1 key contract and Phase 1A through
1H storage units exist. No database consistency claims are made because there is
not yet a complete key-value engine. This document describes the strategy the
remaining implementation will be held to; sections marked *planned* are plans,
not results.

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

Coverage: crash before an fsync, crash mid-append (torn record), crash during a
flush, crash during a compaction, crash between writing a file and updating the
manifest, crash during a range split, crash while a transaction is prepared.

### 2.5 Fault injection

*Planned (Phase 10).* A controllable framework supporting `KillNode`,
`RestartNode`, `PauseNode`, `DropMessage`, `DelayMessage`, `DuplicateMessage`,
`PartitionNodes`, `HealPartition`, `ThrottleDisk`, `ThrottleNetwork` and
`CorruptWALRecord`. Faults are scheduled from the run's seed, so a campaign is
reproducible.

Asymmetric partitions get particular attention: they produce the stale-leader
scenarios that a symmetric partition test never reaches, and those are where
consensus implementations most often turn out to be wrong.

### 2.6 Chaos campaigns

*Planned (Phase 10).* Long randomized runs: a cluster under concurrent client
load while faults are injected continuously, with invariants checked during and
after. Campaigns run on a schedule rather than per-commit, and every failure's
seed is committed to the corpus so it becomes a fast deterministic regression
test on every subsequent build.

### 2.7 History-based consistency checking

*Planned (Phase 10).* Clients record every operation as an invocation and a
response with timestamps:

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

---

## 5. What is not tested

Stated plainly, because an unlisted gap reads as a claim:

- **Byzantine faults.** RivetDB assumes crash-stop. A node that lies is out of
  scope.
- **Silent disk corruption beyond checksum coverage.** Checksums detect
  corruption within a record or block. Corruption that a valid checksum would
  also accept — a whole stale-but-consistent file, for instance — is not
  detected.
- **Clock skew as a safety concern.** RivetDB does not use time for safety, so
  there is nothing to test here; but if a leader-lease read path is adopted in
  Phase 3, that changes, and the assumption will be documented and tested.
- **Adversarial input and authentication.** Input is bounds-checked and
  malformed data is rejected, but there is no threat model and no security
  testing.
- **Performance under sustained multi-day load.** Benchmarks are minutes, not
  days. Slow leaks and long-horizon compaction behaviour are not covered.
