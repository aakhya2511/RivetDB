# RivetDB

RivetDB is an experimental database built from first principles. Its local LSM
storage engine, Raft core, durable replicated ranges, static Multi-Raft node,
range catalog, mutation router, replicated MVCC versions, range-local
historical snapshots, Snapshot Isolation transactions with cross-range 2PC,
transaction-safe online range splitting, online replica migration, and a
deterministic workload-aware placement controller are implemented.

Unlike a basic replicated key-value project, RivetDB models independent
replicated key ranges and is designed to support live range splitting, replica
movement, cross-range transactions, and automated hotspot mitigation.

> **Project status: Phase 12 complete — optional advisory AI freeze candidate.**
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
> Phase 1K adds measured frozen iteration, a bounded leased SSTable-reader
> cache, persisted user-key Bloom filters, lower-copy Scan assembly and explicit
> correctness/stress/crash/exhaustive certification tiers.
> Phase 2 adds a deterministic single-group Raft core with independent durable
> state, RequestVote, AppendEntries, InstallSnapshot, quorum commit, ordered
> apply, crash/restart recovery and seeded 3-/5-node adversarial simulation.
> Phase 3 connects one static full-keyspace range to that core through a
> canonical command codec and an explicit WAL-free replicated LSM apply path.
> Raft indexes are storage sequences; a distinct Manifest frontier proves
> contiguous durable local materialization; replicas may flush/compact into
> different physical layouts while retaining identical logical state.
> Phase 4 hosts many such ranges per node behind a durable authoritative static
> catalog, shared bounded transport/schedulers, exact binary-key routing,
> generation checks and range-scoped leader hints. Independent leaders,
> quorums, crashes, restarts and physical LSM layouts are certified per range.
> Phase 5 adds replicated HLC timestamps, historical `GetAt`/`ScanAt`, applied
> MVCC watermarks and lightweight read-only range snapshots. Historical values
> and tombstones survive flush, compaction, reclamation, crash/restart and
> replica convergence; no MVCC version is garbage-collected.
> Phase 6 adds 128-bit transaction IDs, one immutable read timestamp, lazy
> range serving barriers, buffered write overlays, replicated intents and
> transaction records, first-committer-wins conflict checks, epoch-fenced 2PC
> recovery, and atomic commit at one CT across independently replicated ranges.
> Snapshot Isolation is certified; write skew is intentionally allowed.
> Phase 7 adds a statically bootstrapped replicated MetaRange, monotonic
> descriptor/ID authority, new-ID shadow children, transaction fence/drain,
> exact logical MVCC image transfer, original-timestamp parent delta replay,
> a short final fence, atomic catalog cutover, immutable lineage, stale-parent
> redirects, and retained parent transaction-status archives.
> Phase 8 can move a live range replica between nodes using non-voting
> learners, bounded checksummed snapshot/bootstrap state transfer, Raft
> catch-up, joint-consensus membership transition, leadership transfer,
> replicated placement metadata, and crash-safe old-replica retirement.
> Phase 9 adds injected-clock telemetry and integer EWMA rates, immutable
> canonical planning snapshots, hard placement/capacity filters, projected
> load scoring, deterministic move/split/leader plans, replicated action and
> cooldown state, and restart reconciliation through the Phase 7/8 authorities.
> Phase 10 composes every certified subsystem in a deterministic global
> reference model (including a one-million-event heavy campaign) and a real
> five-node durable crash/recovery campaign with bounded replay traces,
> continuous invariants, historical digests and resource audits.
> Phase 11 fixes a reproducible benchmark/profile taxonomy, adds bounded direct
> latency distributions and full-stack scaling coverage, and retains three
> profile-supported changes: an empty-Version point-read fast path, lower-copy
> MemTable scan consumption and allocation-free projected planner scoring.
> Phase 12 adds an optional provider-independent advisor over bounded canonical
> telemetry. It explains evidence and proposes RangeID-only move, split or
> leadership intents; every candidate still passes the unchanged Phase 9
> planner and fresh validator, and the advisor exposes no mutation API.
>
> **There is no distributed read protocol, network database service, server or
> client yet.** Phase 4/5 local inspection and historical snapshots are
> stale-capable and are not a linearizable distributed read API. Everything else described below
> is not implemented — see
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
make check          # format, vet, lint, tidy, normal tests, diff check
make test           # fast deterministic tests
make race           # practical suite under -race -count=2
make stress         # large randomized/reference campaigns
make crash          # subprocess and publication crash campaigns
make exhaustive     # every-byte WAL/SSTable/Manifest campaigns
make certify-local  # every local correctness tier
make certify-raft   # full single-group Raft gate plus local regression
make certify-range  # Phase 3 integration gate plus Raft/local regressions
make certify-multiraft # Phase 4 gate plus every lower regression gate
make certify-mvcc   # Phase 5 MVCC gate plus every lower regression gate
make certify-txn    # Phase 6 transaction gate plus every lower regression gate
make certify-split  # Phase 7 online split gate plus every lower regression gate
make certify-migration # Phase 8 migration gate plus every lower regression gate
make certify-rebalance # Phase 9 controller gate plus every lower regression tier
make chaos-test       # Phase 10 normal model and targeted composition
make chaos-stress     # deterministic one-million-event global model
make chaos-durable    # real five-node FileStore/LSM composition
make certify-chaos    # Phase 10 gate plus every Phase 1-9 gate
make benchmark      # benchmark suite; honors RIVETDB_BENCH_DIR
make cover     # coverage profile and HTML report
make help      # all targets
```

PR CI runs the practical checks; scheduled/manual CI adds the heavy tiers.
`make certify-mvcc` is the complete replicated-MVCC correctness command.

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

The Phase 0 foundation and completed Phase 1–9 units. Each piece exists because
a later phase cannot be tested honestly without it.

| Package | Purpose |
|---|---|
| [`internal/clock`](internal/clock) | `Clock` interface with a system implementation and a deterministic `Mock`. Raft elections, lease expiry, transaction timeouts and rebalancer cooldowns will take a `Clock`, so those subsystems can be tested in microseconds instead of by sleeping. |
| [`internal/testutil`](internal/testutil) | Seeded randomness with an explicitly promoted failing-seed corpus, goroutine-leak detection, bounded polling helpers. |
| [`internal/storage`](internal/storage) | Integrated local LSM engine over internal-key/write-batch codecs, WAL, skip-list MemTables, SSTables, bounded FIFO flush, Manifest/VersionSet authority and version-preserving L0-to-L1 compaction. |
| [`internal/raft`](internal/raft) | Deterministic static-membership Raft with durable term/vote/log, quorum commit, ordered apply, snapshots, crash/restart and an adversarial simulator. |
| [`internal/replicatedrange`](internal/replicatedrange) | One range-scoped Raft replica, WAL-free Raft-index LSM application, MVCC/transaction commands, split fences, shadow/active/retired lifecycle, logical bootstrap and parent replay provenance. |
| [`internal/multiraft`](internal/multiraft) | Range hosting/routing plus replicated metadata, online split/migration coordinators, and the deterministic telemetry/planning/execution/reconciliation controller. |
| [`internal/mvcc`](internal/mvcc) | Canonical 48-bit physical/16-bit logical HLC timestamps with injected time, observation, regression resistance and explicit exhaustion. |
| [`internal/txn`](internal/txn) | Canonical transaction IDs, records, participants, intents and bounded hostile-input-safe protocol codecs. |
| [`internal/invariant`](internal/invariant) | Named, typed assertions so a violation identifies itself, plus an `Expensive()` tier for O(n) structural checks enabled in tests and chaos runs. |
| [`internal/rlog`](internal/rlog) | Structured logging with canonical attribute keys (`node`, `range`, `term`, `index`, `txn`), context propagation, runtime-adjustable level, and a recorder so tests assert on structured events rather than substrings. |

---

## Local benchmark snapshot

Phase 1K profiles and A/B allocation results are recorded in
[the Phase 1K evidence](docs/evidence/phase-1k.md). The available internal APFS
volume was approximately 96% utilized, so disk-sensitive timings are explicitly
labeled constrained-environment baselines rather than representative
performance claims. Benchmark database data can be placed on an external local
SSD with `RIVETDB_BENCH_DIR`; no volume name is hard-coded.

## Phase 11 performance snapshot

Phase 11 measurements on an Apple M4 MacBook Air (10 cores, 16 GiB, Go
1.25.14) found an active-MemTable `Get` median of 150.8 ns, a 1,000-key
`ScanAt` median of 138.7 us, and a 1,000-range controller-plan median of
711.8 us. These are exact-host CPU/allocation microbenchmarks, not production
claims. The internal APFS volume was 99–100% utilized, so durable Put,
replicated mutation, transaction, split, migration, flush, compaction and
restart timings are labeled `CONSTRAINED-ENVIRONMENT BASELINE` and are not
representative disk results. Distributed benchmarks use an in-process
transport, not a real network.

The retained changes improved empty-Version `Get` by 22.2%, reduced ScanAt
allocations by 33.0%, and improved 1,000-range planning by 15.7% while cutting
planner allocations by 45.3% in matched five-sample A/B runs. See
[the complete Phase 11 evidence](docs/evidence/phase-11.md) for boundaries,
ranges, profiles, rejected changes and limitations.

```bash
RIVETDB_BENCH_DIR=/path/on/a/local/volume make benchmark
PROFILE_DIR=/private/tmp/rivetdb-profiles make benchmark-profile
make benchmark-storage
make benchmark-distributed
make benchmark-txn
make benchmark-control
```

No MVCC/transaction-record GC, range merge, real RPC benchmark, linearizable
distributed read or representative disk result is implied by this snapshot.

## Optional advisory AI operator

Phase 12 adds a provider-independent advisory layer that explains bounded
canonical cluster telemetry and proposes NOOP, WAIT, INVESTIGATE, move, split
or leadership-rebalance intents. AI is never correctness authority: it cannot
select split keys or placement targets, mutate metadata, execute controller
actions or participate in recovery. Human approval invokes the unchanged
Phase 9 planner and fresh validator; only a separate certified host could then
submit the resulting existing action.

Certification is entirely offline with a deterministic mock. It covers strict
schema bounds, evidence grounding, invented/stale identities, cooldown and
operation conflicts, provider failure, timeout, cancellation, backpressure,
audit linkage, parser fuzzing, 10,000 randomized advice cycles and advisor
shadow-mode chaos. See [the design](docs/ai-operator.md) and
[Phase 12 evidence](docs/evidence/phase-12.md).

```bash
make advisor-test
make advisor-race
make advisor-fuzz
make advisor-shadow-chaos
make advisor-benchmark
make certify-advisor
```

No live provider adapter or credential is required or included.

---

## Planned capabilities

Everything here is designed and specified but **not yet built**. Each links to
its phase gate.

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
