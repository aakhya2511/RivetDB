# Working on RivetDB

Context for anyone — human or agent — picking up this repository.

RivetDB is an experimental distributed transactional database: custom LSM
storage engine, Multi-Raft replication, MVCC, cross-range transactions, online
range splitting and replica migration, and workload-adaptive rebalancing. Read
[README.md](README.md) for what it is and
[docs/roadmap.md](docs/roadmap.md) for where it stands.

---

## 1. Current state

**Phases 0–5 are complete and certified. Phase 6 distributed transactions is next.**

What exists: the design documents, build/CI gate, foundation packages,
internal-key/write-batch primitives, the checksummed WAL, the concurrent
mutable/frozen MemTable, deterministic SSTable writer/reader and bounded FIFO
flush pipeline and crash-safe Manifest/VersionSet under `internal/storage`.
There is a complete local latest-state engine plus an independent deterministic
Raft core, memory/file Raft stores, simulator and snapshot foundation under
`internal/raft`. `internal/replicatedrange` composes one range with a WAL-free
Raft-index apply path, explicit durable local frontier and bounded proposal
waiters. `internal/multiraft` adds the authoritative static catalog, per-node
hosting, shared bounded transport/schedulers and routed mutations. There is no
server, client, distributed read protocol or dynamic range metadata.
`internal/mvcc` plus the Phase 5 replicated-range/storage extensions provide
replicated HLC versions and range-local historical read-only snapshots. There
are no intents, write transactions, 2PC, isolation guarantee or MVCC GC.

The module has **zero dependencies** and no `go.sum`. Keep it that way as long
as it is honest to; §24 of the project brief allows dependencies for
infrastructure concerns (RPC, protobuf, metrics) but every one must solve a real
requirement.

Git: branch `main`, **no remote configured**.

---

## 2. Environment

This machine had no Go toolchain and no Homebrew. Both were installed manually
and **neither is on `PATH`**. Every command below assumes:

```bash
export PATH="$HOME/sdk/go1.25.14/bin:$PWD/bin:$PATH"
export GOTOOLCHAIN=local
```

| Tool | Location | Notes |
|---|---|---|
| Go 1.25.14 | `~/sdk/go1.25.14/bin/go` | **Use this one.** It matches the `go.mod` floor and CI. |
| Go 1.27.1 | `~/sdk/go1.27.1/bin/go` | Forward-compatibility checks only. |
| golangci-lint v2.6.1 | `./bin/golangci-lint` | Gitignored, not committed. |

**Known toolchain trap:** golangci-lint v2.6.1 is built against Go 1.25 and
cannot decode Go 1.27 export data. Running it under Go 1.27 fails with
`could not load export data ... version 4 is greater than maximum supported
version 2`. That is not a code problem. Run lint with Go 1.25.14.

If `bin/golangci-lint` is missing:

```bash
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
  | sh -s -- -b "$PWD/bin" v2.6.1
```

---

## 3. Commands

```bash
make check    # format, vet, lint, tidy, normal tests and diff check
make test     # fast deterministic tests
make race     # practical race suite, count 2
make stress   # large randomized/reference campaigns
make crash    # subprocess and publication crash campaigns
make exhaustive # every-byte truncation/corruption campaigns
make certify-local # complete local-storage correctness gate
make raft-test # deterministic Raft core and persistence tests
make raft-race # Raft tests under the race detector
make raft-stress # 100k-event simulation and durable-store stress
make raft-chaos # fixed/fresh adversarial cluster schedules
make raft-exhaustive # every-byte Raft-store campaigns
make certify-raft # complete Phase 2 gate plus certify-local
make benchmark # benchmark suite; honors RIVETDB_BENCH_DIR
make cover    # coverage profile + HTML report in bin/
make tidy     # go mod tidy, fails if it was not already tidy
make help     # everything else
```

PR CI runs the practical tiers; scheduled/manual CI adds stress, crash and
exhaustive. `make certify-local` runs every local-storage correctness tier.

Replaying a randomized failure:

```bash
make seed SEED=8134472901 RUN=TestSomething
RIVETDB_SEED=8134472901 go test -run TestSomething ./...
```

Normal tests never modify the committed seed corpus. After reproducing a
failure, promote it explicitly:

```bash
make promote-seed PACKAGE=./internal/package TEST=TestSomething SEED=8134472901
```

---

## 4. The working rule

This is the process the project is built on. It is not optional decoration —
the phase gates are what make the correctness claims credible.

For every phase, in order:

1. Read the existing code before designing.
2. Write or revise the design note in `docs/`.
3. State the invariant the phase must preserve, in `docs/invariants.md`.
4. Implement the smallest correct unit.
5. Write the tests, **including the failure and crash cases**.
6. Run them. Run the race detector and the linters.
7. Run the phase acceptance scenario from `docs/roadmap.md`.
8. Record the evidence.
9. Commit only once the gate passes.

Do not start phase *N+1* before phase *N*'s gate passes. Correctness before
breadth: a half-working feature that demos well is worth less than a complete
one with its failure modes tested.

---

## 5. Conventions

These are enforced by review and, where possible, by the linter. Violating them
silently is worse than not following them at all.

**Time.** Code under `internal/` must not call `time.Now`, `time.After`,
`time.NewTimer` or `time.NewTicker`. Take a
[`clock.Clock`](internal/clock/clock.go). This exists so that Raft elections,
lease expiry, transaction timeouts and rebalancer cooldowns are testable in
microseconds and reproducibly, rather than by sleeping. Passing a real clock
into a test is a design smell.

**Randomness.** Tests draw seeds from
[`testutil.Seed`](internal/testutil/seed.go). Never share one seeded
`*rand.Rand` across goroutines — it makes the *order of draws* depend on the
scheduler, so the seed stops determining the run. Derive one source per worker,
or generate the schedule up front.

**Invariants.** Safety properties have stable IDs in
[docs/invariants.md](docs/invariants.md) (`RAFT-4`, `STORAGE-6`, …). Runtime
assertions use those IDs:
`invariant.Assert(cond, "RAFT-7", "commit index %d < applied %d", c, a)`.
Assertions are for properties whose violation means correctness is already lost;
they panic. Corrupt input, full disks, stale routes and rejected RPCs are normal
conditions and are returned as errors. O(n) structural checks go behind
`invariant.Expensive()`.

**Errors.** Wrap with context (`wrapcheck` enforces it). Use sentinel errors,
not formatted strings, for conditions callers branch on (`err113` enforces it) —
recovery code has to distinguish "corrupt record" from "short read" via
`errors.Is`.

**Logging.** Use [`internal/rlog`](internal/rlog) and its canonical attribute
keys (`node`, `range`, `term`, `index`, `txn`). Consistent keys are what let a
log query follow one range across every node, which is the query you need when
debugging a failed split. Tests assert on structured records via
`rlog.Recorder`, not on substrings.

**Concurrency.** Every component that starts goroutines exposes `Close`, cancels
a context, and waits. Tests use `defer testutil.NoLeaks(t)()`. All inter-goroutine
channels are bounded — an unbounded queue turns overload into an
out-of-memory crash. Prefer single-writer state machines and immutable snapshots
over shared mutable state.

**Honesty.** No placeholder code presented as complete. No `TODO`-based fake
implementations. If a subsystem is incomplete, say so in its doc and in the
README. Never substitute a simpler algorithm and claim the requested one exists.
Never publish a performance number that was not measured, and never claim
linearizability or serializability without a checker that demonstrates it.

**Repository shape.** Directories are created when they have code in them. Do
not scaffold empty packages to match a template.

---

## 6. Document map

Design is written before the code it governs.

| Document | What it settles |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Layer boundaries, control plane, failure model, concurrency model, and §14's open questions |
| [docs/invariants.md](docs/invariants.md) | Safety properties, with the IDs assertions and tests reference |
| [docs/storage-engine.md](docs/storage-engine.md) | Phase 1 spec: byte-level on-disk formats, durability modes, crash-scenario table, test list |
| [docs/raft.md](docs/raft.md) | Phase 2 protocol, persistence, simulator, snapshot and Phase 3 boundaries |
| [docs/replicated-range.md](docs/replicated-range.md) | Phase 3 Raft-to-LSM ordering, durability, replay and runtime contract |
| [docs/multiraft.md](docs/multiraft.md) | Phase 4 node composition, static metadata authority, shared runtime and failure domains |
| [docs/range-routing.md](docs/range-routing.md) | Phase 4 descriptors, catalog format, routing and future split/migration seams |
| [docs/mvcc.md](docs/mvcc.md) | Phase 5 HLC authority, historical visibility, snapshots, watermarks and Phase 6 boundary |
| [docs/correctness.md](docs/correctness.md) | Test strategy, reproducibility mechanism, and §5's explicit list of what is *not* tested |
| [docs/roadmap.md](docs/roadmap.md) | The twelve phases and each gate |
| [docs/design-decisions/](docs/design-decisions/) | ADRs. An ADR records a contested decision with the alternatives that lost, and is superseded rather than rewritten. |

Decisions recorded: [ADR-0001](docs/design-decisions/0001-lsm-tree-over-b-tree.md)
(LSM over B+ tree),
[ADR-0002](docs/design-decisions/0002-range-partitioning-over-hashing.md)
(range partitioning over consistent hashing), and
[ADR-0003](docs/design-decisions/0003-explicit-internal-key-comparator.md)
(explicit internal-key comparison),
[ADR-0004](docs/design-decisions/0004-wal-integrity-and-tail-recovery.md)
(WAL integrity and tail recovery), and
[ADR-0005](docs/design-decisions/0005-memtable-skip-list.md)
(MemTable skip list), and
[ADR-0006](docs/design-decisions/0006-sstable-physical-format.md)
(SSTable physical format), through
[ADR-0014](docs/design-decisions/0014-raft-core-persistence-and-apply-boundary.md)
(Raft core persistence and apply boundary), and
[ADR-0015](docs/design-decisions/0015-raft-to-lsm-replicated-state-machine.md)
(Raft-to-LSM integration), and
[ADR-0016](docs/design-decisions/0016-multiraft-static-range-routing.md)
(static Multi-Raft hosting and routing), and
[ADR-0017](docs/design-decisions/0017-hlc-mvcc-timestamp-authority.md)
(HLC MVCC timestamp authority). The ADR index lists the complete
sequence. They list the rejected options' genuine
advantages, not strawmen — keep that standard.

---

## 7. Phase boundaries and next work

The spec is [docs/storage-engine.md](docs/storage-engine.md); §11 is the test
list that constitutes the gate. Suggested build order, smallest correct unit
first:

1. Internal key encoding (§5.2) — with the ordering property tested against a
   reference comparator over randomized inputs. Complete.
2. WAL record framing and replay (§5.1) — fragmentation across block
   boundaries and strict stop at corruption. Complete.
3. MemTable. Complete.
4. SSTable format and writer (§5.3). Complete.
5. Production SSTable reader, seek and iteration. Complete. Bloom-filter
   construction/query remains deferred.
6. MemTable rotation and SSTable flush pipeline. Complete.
7. Manifest and version set (§5.4). Complete; see `docs/evidence/phase-1g.md`.
8. Version-preserving L0-to-L1 compaction (§5.5). Complete; see `docs/evidence/phase-1h.md`.
9. Integrated recovery and local read path. Complete; see `docs/evidence/phase-1i.md`.
10. Crash recovery, reclamation safety and deep local-engine stress. Complete;
    see `docs/evidence/phase-1j.md`.
11. Profiling, performance engineering and final local-storage certification.
    Complete; see `docs/evidence/phase-1k.md` and ADR-0013.

Benchmark data uses `testing.TempDir` by default. Set
`RIVETDB_BENCH_DIR=/Volumes/<volume>/rivetdb-bench` to place only benchmark
database/test data on an external local SSD. Never hard-code a volume name or
publish disk-sensitive numbers without recording filesystem, capacity, free
space, utilization, directory, hardware, OS, Go and power state.

Two parts of the spec are load-bearing and should not be changed casually:

- **§5.2's key encoding and comparator.** The complemented sequence number
  makes versions newest-first only after user-key equality is established.
  Arbitrary-length user keys require the explicit comparator in ADR-0003; raw
  `bytes.Compare` on encoded internal keys is forbidden. Phase 5's MVCC
  inherits this contract and file format.
- **§5.4's durability ordering.** Contents fsynced, *then the containing
  directory fsynced*, and only then referenced from the manifest. The directory
  fsync is the step usually forgotten, and without it a rename can be lost
  across a crash even though the file's contents were durable.

Open questions retained for their owning later phases:

- **Shared WAL across ranges.** Phase 4 deliberately retains one LSM and Raft
  store per range. Replicated apply does not use the data WAL; any later
  group-commit/shared-log design must preserve range isolation and recovery.
- **No range merging** is planned. A workload that creates many ranges and goes
  quiet leaves them fragmented permanently. Stated as a limitation in the
  README; add it to the roadmap if the Phase 9 demo needs it.
- **Read path** (Raft read vs ReadIndex vs leader lease) remains undecided for
  Phase 5. Phase 4 exposes routed mutations and stale-capable local inspection,
  not a distributed read surface. Architecture §11 assumes nothing about clock
  skew, while leader leases would make safety depend on clock bounds. See §14.

Phase 5 is frozen at [docs/mvcc.md](docs/mvcc.md): 48/16 HLC timestamps carried
in replicated commands, distinct Raft/MVCC authorities, local applied-read
watermarks, version-preserving history, read-only range snapshots and no GC or
distributed-read freshness claim. Run `make certify-mvcc`; it includes every
lower regression gate. Phase 6 must not weaken the Phase 1 comparator, Phase 3
durability boundary, Phase 4 ownership, or Phase 5 historical visibility.
