# Correctness Strategy

A distributed database is only as credible as the evidence that it is correct.
This document describes how RivetDB produces that evidence, and — equally
important — states the boundary of what is currently checked.

**Current scope:** only the Phase 0 foundation exists. No consistency claims
are made about RivetDB, because there is no database yet. This document
describes the strategy the implementation will be held to, and the sections
marked *planned* are plans, not results.

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

Durability is not testable by a normal test, because a normal test never stops
at an arbitrary instruction. RivetDB tests it by making the crash point
explicit: the storage engine's file operations go through an injectable layer
that can fail or stop at a chosen write offset, so a test can crash the engine
at every offset in a WAL segment and assert the recovered state is a valid
prefix at each one.

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
- A failure appends the seed to `testdata/seeds/<TestName>.seeds` and prints
  the exact replay command.
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
