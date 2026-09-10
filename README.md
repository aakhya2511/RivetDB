# RivetDB

RivetDB is an experimental distributed transactional database built from first
principles around a custom LSM storage engine, Multi-Raft replication, MVCC,
and workload-adaptive range rebalancing.

Unlike a basic replicated key-value project, RivetDB models independent
replicated key ranges and is designed to support live range splitting, replica
movement, cross-range transactions, and automated hotspot mitigation.

> **Project status: Phase 1 in progress — Phase 1A through Phase 1J complete.**
>
> What exists today is the documented design, the build and CI gate, the
> testing foundation, the authoritative internal-key/write-batch primitives,
> a durable checksummed WAL with streaming recovery and explicit tail repair,
> a concurrent mutable/frozen skip-list MemTable, a deterministic checksummed
> and atomically published SSTable writer, and a fully validating,
> comparator-correct SSTable reader with seek and iteration, and a bounded
> active-to-immutable MemTable rotation/flush pipeline, and the crash-safe
> Manifest/immutable VersionSet authority with durable table installation.
> Phase 1I adds an integrated local LSM engine with Open, Put, Delete, Get,
> Scan, Flush, Compact, Close and restart recovery. Phase 1J separates assigned
> and published visibility, adds real subprocess crash coverage, conservative
> obsolete-SSTable reclamation and a 50,000-operation/120-restart stress gate.
>
> **There is no Raft, distributed database service, server or client yet.**
> Profiling, optimization and final local-storage certification are next. Everything
> else described below is a design with a written specification, not working code — see
> [Roadmap](docs/roadmap.md) for exactly what is built and what is not.
>
> RivetDB is a research and demonstration system. It is not production
> software and has no upgrade, backup or operational story.

---

## Why this project exists

Most distributed key-value projects stop once writes replicate and a leader
election works. That leaves the interesting question untouched:

> When a workload concentrates on a small part of the keyspace, can a database
> reshape its own data distribution — splitting ranges, moving replicas,
> redistributing leadership — while continuing to serve reads and writes
> correctly?

Answering that requires the layers underneath to be real. A range cannot be
split online without a storage engine whose files are immutable and a
consensus layer that can order the split against concurrent writes. So RivetDB
implements those layers itself rather than delegating them: the storage engine,
the Raft implementation, the MVCC layer and the transaction protocol are all in
this repository.

---

## Architecture

```text
                         ┌──────────────┐
                         │    Client    │
                         │  GET / PUT   │
                         │ DELETE/SCAN  │
                         │ BEGIN/COMMIT │
                         └──────┬───────┘
                                ▼
                    ┌───────────────────────┐
                    │   Router / Gateway    │  key → range → leader,
                    │                       │  retry on stale route
                    └───────────┬───────────┘
                                ▼
                    ┌───────────────────────┐
                    │  Range  [start, end)  │  one shard of the keyspace
                    └───────────┬───────────┘
                                ▼
                    ┌───────────────────────┐
                    │      Raft group       │  one per range, independent
                    └───────────┬───────────┘
                                ▼
                    ┌───────────────────────┐
                    │      MVCC layer       │  versions, intents, snapshots
                    └───────────┬───────────┘
                                ▼
                    ┌───────────────────────┐
                    │  LSM storage engine   │  WAL → MemTable → SSTables
                    └───────────────────────┘
```

The control plane runs on the same nodes, observing the data plane and
reshaping it:

```text
   per-range telemetry  ──►  rebalancer  ──►  safety validator  ──►  execution
   QPS, bytes, size,         detect and       replication factor,     split range
   latency percentiles,      propose          quorum, capacity,       move replica
   leader count, lag         (deterministic)  cooldown, concurrency   transfer leader
```

The validator is the only path to execution. Every proposal passes it,
regardless of origin — including the optional AI operator, which recommends and
never executes.

Full detail: [docs/architecture.md](docs/architecture.md).

---

## Design documents

The design is written before the code, so each subsystem has a specification
that the implementation and its tests are held to.

| Document | Contents |
|---|---|
| [architecture.md](docs/architecture.md) | System structure, layer boundaries, failure model, concurrency model, open questions |
| [invariants.md](docs/invariants.md) | Every safety property, with a stable ID used by runtime assertions and tests |
| [storage-engine.md](docs/storage-engine.md) | Phase 1 design: byte-level on-disk formats, durability modes, crash scenarios |
| [correctness.md](docs/correctness.md) | Testing strategy, reproducibility mechanism, and what is *not* tested |
| [roadmap.md](docs/roadmap.md) | The twelve phases and the gate each must pass |
| [ADRs](docs/design-decisions/) | Contested decisions with the alternatives that were rejected and why |

The [ADR index](docs/design-decisions/) records the accepted choices and the
real advantages of their rejected alternatives.

---

## Building and testing

Requires Go 1.25 or later. No other dependencies — the module currently has
none.

```bash
make check     # the full gate: format, vet, lint, tests, race tests
make test      # fast tests
make race      # go test -race -count=2 ./...
make cover     # coverage profile and HTML report
make help      # all targets
```

`make check` runs exactly what CI runs, so a green local check should mean a
green pipeline.

### Reproducing a randomized failure

Randomized tests log their seed. A failure prints exact commands to replay it
and, after reproduction, explicitly promote it into `testdata/seeds/` next to
the package that failed. Replay it with:

```bash
make seed SEED=8134472901 RUN=TestSomething
# or
RIVETDB_SEED=8134472901 go test -run TestSomething ./...
```

After reproducing the failure, promote it deliberately:

```bash
make promote-seed PACKAGE=./internal/storage TEST=TestSomething SEED=8134472901
```

Committed seeds are replayed by the tests that recorded them, so a bug found
once by chance becomes a deterministic regression test without ordinary test
or CI execution modifying the working tree.

---

## What is implemented today

The Phase 0 foundation and completed Phase 1 units. Each piece exists because
a later phase cannot be tested honestly without it.

| Package | Purpose |
|---|---|
| [`internal/clock`](internal/clock) | `Clock` interface with a system implementation and a deterministic `Mock`. Raft elections, lease expiry, transaction timeouts and rebalancer cooldowns will take a `Clock`, so those subsystems can be tested in microseconds instead of by sleeping. |
| [`internal/testutil`](internal/testutil) | Seeded randomness with an explicitly promoted failing-seed corpus, goroutine-leak detection, bounded polling helpers. |
| [`internal/storage`](internal/storage) | Integrated local LSM engine over internal-key/write-batch codecs, WAL, skip-list MemTables, SSTables, bounded FIFO flush, Manifest/VersionSet authority and version-preserving L0-to-L1 compaction. |
| [`internal/invariant`](internal/invariant) | Named, typed assertions so a violation identifies itself, plus an `Expensive()` tier for O(n) structural checks enabled in tests and chaos runs. |
| [`internal/rlog`](internal/rlog) | Structured logging with canonical attribute keys (`node`, `range`, `term`, `index`, `txn`), context propagation, runtime-adjustable level, and a recorder so tests assert on structured events rather than substrings. |

---

## Planned capabilities

Everything here is designed and specified but **not yet built**. Each links to
its phase gate.

- **Custom LSM storage engine** — WAL with per-record checksums, MemTable,
  SSTables with sparse index and Bloom filters, levelled compaction, manifest,
  crash recovery. [Design](docs/storage-engine.md) ·
  [Phase 1](docs/roadmap.md#phase-1--local-storage-engine-)
- **Raft** — implemented here, not imported, because the failure handling is
  the point. [Phase 2](docs/roadmap.md#phase-2--single-raft-group-)
- **Multi-Raft and range routing** — one Raft group per key range, many ranges
  per node. [Phase 4](docs/roadmap.md#phase-4--multi-raft-and-routing-)
- **MVCC and distributed transactions** — write intents, snapshot reads,
  two-phase commit over replicated transaction records.
  [Phases 5–6](docs/roadmap.md#phase-5--mvcc-)
- **Online range splitting and replica migration** — reconfiguration as
  restartable state machines, with the data plane serving throughout.
  [Phases 7–8](docs/roadmap.md#phase-7--range-splitting-)
- **Workload-aware rebalancing** — the signature feature, with a before/after
  experiment on a skewed workload.
  [Phase 9](docs/roadmap.md#phase-9--workload-aware-rebalancer-)
- **Chaos and correctness campaigns** — fault injection, seeded randomized
  campaigns, history-based consistency checking.
  [Phase 10](docs/roadmap.md#phase-10--chaos-and-correctness-)

---

## Consistency and failure model

Stated now so the implementation can be held to it.

**Failure assumptions.** Crash-stop with recovery; an asynchronous network that
may drop, delay, reorder and duplicate messages; asymmetric partitions;
unsynchronised clocks, with time used for liveness but never for safety; disks
that may corrupt or tear writes, detected by checksums.

**Isolation.** The initial target is snapshot isolation, which permits write
skew. RivetDB will not be described as serializable or linearizable unless a
checker demonstrates it on recorded histories. See
[correctness.md](docs/correctness.md) §1.

**Known limitations.** No range merging, so a workload that creates many ranges
and then goes quiet leaves them fragmented. No SQL. No authentication or
encryption. No geo-distribution. MVCC garbage collection is required for
bounded storage growth and does not exist yet. The full list of untested areas
is in [correctness.md](docs/correctness.md) §5.

---

## Contributing

RivetDB is a personal systems project rather than a community effort, but the
working rules are written down in [roadmap.md](docs/roadmap.md#working-rules):
design before code, invariant before implementation, tests including the
failure cases, and a gate that must pass before a commit.

## License

Apache License 2.0. See [LICENSE](LICENSE).
