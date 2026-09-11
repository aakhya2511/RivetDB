# Phase 6 certification evidence

Date: 2026-09-11  
Baseline commit: `5b136cfbf99a6ff59c3b1b06f6d7e5f2cee88d01`

## Result and claim boundary

`PHASE 6 DISTRIBUTED TRANSACTION DESIGN GATE: PASS`

`PHASE 6: PASS`

The certified claim is Snapshot Isolation read/write transactions with atomic
commit across independently Raft-replicated ranges, using replicated records,
durable intents, 2PC and epoch-fenced recovery. Write skew is deliberately
allowed. This is not serializability, strict serializability, external
consistency or a linearizable-read claim.

## Deterministic protocol evidence

- Two- and three-range transactions materialized every write at one CT, with
  `CT > RT`, and the participant records agreed with the home record.
- An eleven-participant transaction committed. Twelve same-key contenders had
  exactly one winner. A separate 24-way concurrent mix covered conflicting,
  disjoint, single-range, two-range and three-range transactions: one hot-key
  winner and eighteen disjoint winners.
- The explicit SI anomaly committed two transactions that read the same
  snapshot and wrote disjoint keys. This is legal write skew and demonstrates
  why no serializability claim is made.
- Read-your-writes and scan overlays covered replacement, insertion and
  deletion. A read-only commit created no transaction record.
- Pending, partially prepared, fully prepared, committed/partially resolved and
  aborted/partially resolved records survived a full five-node restart.
  Recovery epoch-fenced and aborted undecided work, and completed terminal
  resolution.
- A canceled client wait immediately after the durable COMMITTED decision
  returned an error. Lookup by TxnID returned COMMITTED with its CT; retry on
  the same identity completed resolution without a second transaction.
- Leader changes during prepare/decision/resolution, a range-specific quorum
  failure, stale coordinator messages, wrong-range prepare, duplicate protocol
  operations and malformed intent interpretation were tested.
- Engine tests flushed, restarted, compacted and resolved value/delete intents.
  Historical reads before CT remained unchanged; abort markers fell through to
  older committed history.
- Concurrent timestamp order uncovered and fixed a Manifest watermark issue:
  each published replicated-MVCC table now advances the cumulative maximum
  timestamp rather than regressing to that table's local maximum. A focused
  Manifest regression covers `100` followed by `50`.

## Crash matrix

The opt-in test used child processes, real Raft FileStores, real MVCC LSMs,
Manifests and abrupt `os.Exit(77)` at all seven required boundaries:

| Crash after | Recovered decision | Account state |
|---|---|---|
| PENDING durable | ABORTED | 1000 / 500 |
| participant 1 prepared | ABORTED | 1000 / 500 |
| all participants prepared | ABORTED | 1000 / 500 |
| COMMIT decision durable | COMMITTED | 800 / 700 |
| participant 1 resolved | COMMITTED | 800 / 700 |
| all participants resolved | COMMITTED | 800 / 700 |
| ABORT decision durable | ABORTED | 1000 / 500 |

No recovered state exposed a partial transfer.

## Randomized Snapshot Isolation evidence

Normal campaign: five nodes, three ranges, three seeds (`1`, `8134472901`, and
fresh seed `-5697089307059434870`). Each seed executed 3,604 events, thirteen
transactions, nine commits, four aborts, two write conflicts, one coordinator
crash, one participant crash, one full restart, one range partition, three
leader changes, two flushes, two compactions and sixty historical checks. Total:
10,812 events, 39 transactions, 27 commits, 12 aborts and six conflicts, with
zero atomicity violations and zero SI violations.

Heavy replay seed `8931604308602070411` executed 100,014 events, 44
transactions, 32 commits, 12 aborts, seven conflicts, three coordinator
crashes, three participant crashes, three restarts, four partitions, nine
leader changes, six flushes, six compactions and 210 historical checks. It
reported zero atomicity violations and zero SI violations. Replay:

```text
RIVETDB_TXN_STRESS=1 RIVETDB_SEED=8931604308602070411 \
  go test -run '^TestRandomizedTransactionsHeavy$' ./internal/multiraft
```

## Certification commands

The final composed `make certify-txn` run passed. It executed:

- normal full tests, gofmt check, `go vet`, golangci-lint and tidy/diff checks;
- `txn-test`, `txn-race`, fresh-seed `txn-stress`, `txn-chaos`, and the
  real-process `txn-crash` matrix;
- `certify-mvcc`, `certify-multiraft`, `certify-range`, `certify-raft`, and
  `certify-local`, including their race, stress, chaos, crash and exhaustive
  sub-gates.

Independent `go vet ./...` runs passed under Go 1.25.14 and Go 1.27.1.
Golangci-lint v2.6.1 reported `0 issues` under Go 1.25.14.

## Constrained-environment performance baseline

These are `CONSTRAINED-ENVIRONMENT BASELINE` measurements for engineering only,
not representative disk-performance claims. Each benchmark ran exactly once
(`-benchtime=1x`) and therefore has no statistical confidence:

| Case | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| read-only | 21,073,667 | 21,384 | 264 |
| single-range | 188,746,292 | 211,608 | 2,443 |
| two-range | 287,141,208 | 317,376 | 3,847 |
| three-range | 393,183,750 | 431,064 | 5,309 |

Recorded environment: Apple M4 MacBook Air, 10 cores, 16 GiB RAM; macOS 15.7.4
(24G517), arm64; APFS data volume `/dev/disk3s5`, 228 GiB capacity, 175 GiB
reported used, 9.0 GiB available and 96% utilization after certification.
Benchmark data used `testing.TempDir` beneath
`/var/folders/md/5ddbc0713d17ftbrw0jxvy4w0000gn/T/`; no external SSD was
mounted and `RIVETDB_BENCH_DIR` was unset. Go versions were 1.25.14 and 1.27.1.
Power at final recording was AC, 99%, finishing charge.

APFS free-space reporting varied during the run (as low as 1.3 GiB before
temporary test cleanup). Consequently no fsync, flush or compaction number from
this host is presented as final representative performance. The configurable
`RIVETDB_BENCH_DIR` path supports placing only benchmark-owned data on an
external local SSD in a future performance run.

## No-GC and future-phase boundary

Version GC: no. Tombstone GC: no. Transaction-record GC: no. Intent cleanup
occurs only after authoritative COMMITTED or ABORTED resolution. Phase 7 must
add descriptor-generation pinning, split/prepare ordering, durable
parent-to-child participant redirects, child prepared-state construction and
crash-safe recovery traversal. Phase 8 must preserve records, participants and
intents during replica migration. Neither feature is implemented here.
