# Phase 2 evidence: Raft consensus core and replicated log

Date: 2026-09-10. Phase 1 parent:
`8cc016cdd5c519ae552dd9ffe9f7d48928d62042`. Certification source is the
Phase 2 commit containing this record.

## Claim and boundary

Phase 2 implements and mechanically qualifies one static-membership Raft group
against an injected deterministic state machine. The core has follower,
candidate and leader roles; RequestVote, AppendEntries and InstallSnapshot;
quorum/current-term commit; ordered apply; restart; and snapshot/log compaction.

This is not a replicated RivetDB database. `internal/raft` neither imports
`internal/storage` nor calls `Engine.Put`/`Engine.Delete`; it does not allocate
Phase 1 storage sequence numbers or use the data WAL. The LSM, Multi-Raft,
network service, MVCC, transactions and distributed reads remain future work.

## Architecture qualified

The core is synchronous and single-owner. A `Tick`, proposal or RPC is one
serialized event. It reads no wall clock and performs no transport I/O.
Randomized election timeouts use an injected source; the simulator maps an
injected `clock.Clock` to logical ticks and owns directed links and delivery.
Outbound messages are values returned by the core and can be dropped without
rolling back durable state.

Persistent state is one atomic `PersistentState`: hard term/vote, snapshot
index/term/payload and contiguous typed log entries. Volatile role, leader,
commit/applied indexes, timers, votes and leader match/next indexes are rebuilt
after restart. A new node starts at term/index zero; a restarted node is a
follower and knows commitment only through its durable snapshot until a leader
communicates a later commit.

`MemoryStore` is explicitly non-durable. `FileStore` is an independent complete-
state store, not the LSM WAL. `RAFTSTATE` has an explicit little-endian v1
format, bounded lengths and entry count, and CRC32C. Publication is temporary
write, file fsync, close, rename, then containing-directory fsync. Hard state
and log changes are saved before dependent messages; uncertainty stops the node.

## Deterministic scenario evidence

Targeted tests cover:

- one-node self-election/no-op/proposal/commit/apply;
- healthy three- and five-node election and identical state-machine digests;
- four-node quorum of three and a five-node leader-plus-one minority;
- split-vote retry, higher-term step-down and stale-term rejection;
- exact vote freshness and a candidate missing a committed entry;
- one/many-entry replication, heartbeat commit propagation and 100-entry lag;
- old `A,B,X,Y` suffix replacement by `A,B,C,D,E` without committed truncation;
- isolated leader, majority election/progress, heal, step-down and repair;
- duplicate, reordered and asymmetric message delivery;
- node and full-cluster restart with durable term/vote/log recovery;
- the classic current-term commit rule;
- no apply before commit, ordered/nonduplicate apply and fatal apply failure;
- snapshot create/install, stale rejection, matching-suffix preservation,
  restart and post-snapshot AppendEntries resumption.

The simulator checks RAFT-1 election history, RAFT-2 leader-log extension,
RAFT-3 matching prefixes, RAFT-4 committed-entry presence in later leaders,
RAFT-5 committed/applied identity, RAFT-6 term monotonicity, RAFT-7
apply/commit/log bounds, RAFT-8 vote history, RAFT-11 snapshot bounds and
RAFT-13 duplicate application at every event. Targeted schedules establish
RAFT-9/10/12/14.

## Randomized campaigns

The normal fresh campaign ran 10,000 events for each fixed/fresh seed and each
cluster size, 60,000 events total:

| Nodes | Seed | Proposals | Link changes | Crashes | Restarts | Leader terms | Committed indexes |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 3 | 101 | 97 | 513 | 220 | 218 | 34 | 119 |
| 3 | 9901 | 115 | 528 | 204 | 203 | 35 | 141 |
| 3 | 3059939862677632797 | 144 | 505 | 224 | 222 | 33 | 155 |
| 5 | 101 | 50 | 501 | 270 | 270 | 15 | 55 |
| 5 | 9901 | 34 | 527 | 255 | 251 | 13 | 38 |
| 5 | 3059939862677632797 | 20 | 474 | 266 | 264 | 11 | 28 |

All converged after heal with identical logical digests; invariant violations:
zero. Replay the fresh seed with:

```text
RIVETDB_SEED=3059939862677632797 go test -run '^TestRandomizedClusterSafety$' ./internal/raft
```

The opt-in fresh five-node campaign ran 100,000 events with seed
`-2285975210833422349`: 314 proposals, 5,035 directed-link changes, 2,693
crashes, 2,693 restarts, 89 leader terms and 341 observed committed indexes.
It converged with zero invariant violations.

## Persistence and crash evidence

The file-store suite proves exact save/reopen byte ownership; every truncation
and single-byte corruption is rejected; checksum-valid index gaps, decreasing
terms, invalid votes and malformed state fail startup. A partial unpublished
`RAFTSTATE.tmp` never supersedes the prior authority. An injected matrix covers
temporary open/write/fsync/close, rename and directory open/fsync/close, and
asserts the successful publication order including directory fsync.

Node/store integration persists a vote, restarts, and rejects a different
candidate in that term. It persists follower appends before success and leader
local proposals before replication. Injected save failure yields no dependent
message and makes the node stopped. Whole-state atomic replacement means a
crash before publication recovers the prior valid state; after directory fsync
it recovers the new complete state, never a torn acknowledged suffix.

The opt-in durable-store campaign used seed `-4850343182157323552` for 500
save/reopen transitions, ending at term 30 with snapshot index 200 and 300
retained entries. It passed. Because the APFS data volume was 95% utilized and
power reported AC while the battery was discharging, elapsed time is not a
representative disk-performance result.

## Engineering baselines and environment

| Item | Recorded value |
|---|---|
| Hardware | MacBook Air `Mac16,12`, Apple M4, 10 cores, 16 GB RAM |
| OS | macOS 15.7.4 (24G517), Darwin arm64 |
| Filesystem | local APFS data volume, 228 GiB capacity |
| Free/utilization | 11 GiB free / 95% utilized |
| Benchmark/test root | Go temporary directories on the same APFS volume |
| External SSD | not used |
| Power | AC reported; battery 83% and discharging |
| Go | 1.25.14 primary; 1.27.1 compatibility gate |

Three 500 ms samples on the certified core produced an in-memory three-node
propose→commit→local-apply range of 35.482–47.851 µs/op (median 39.860 µs,
45,144 B and 461 allocations/op). Encoding and decoding a 100,000-entry
`RAFTSTATE` took a median 3.164 ms at 1,390.88 MB/s (7,200,514 B and 100,001
allocations/op).
These are development baselines, not network, failover, throughput or
production-latency claims. No fsync latency number is published.

## Test tiers

- `make raft-test`: deterministic core, cluster, persistence and snapshot tests.
- `make raft-race`: the Raft suite twice under the race detector.
- `make raft-stress`: 100,000-event simulation plus durable-store stress.
- `make raft-chaos`: fixed/fresh partitions, isolation, lag and message faults.
- `make raft-exhaustive`: every-byte store corruption and publication failures.
- `make certify-raft`: all repository checks and Raft tiers, followed by the
  unchanged `make certify-local` Phase 1 regression.

The scheduled/manual CI heavy matrix includes all three opt-in Raft tiers.

## Phase 3 integration-readiness audit

1. A committed user command is an immutable `EntryCommand` containing index,
   term and opaque logical command bytes; no-op entries carry no command.
2. Raft log order is the replicated ordering authority.
3. Use Raft log index as the logical mutation sequence. No-op/config entries
   consume indexes but do not mutate storage; gaps in mutation sequences are
   harmless and preserve consensus provenance.
4. Current `Engine.Put`/`Engine.Delete` cannot be reused: they allocate a
   replica-local sequence and append the local data WAL, so replicas could
   derive different logical order and create an unproved second authority.
5. Preferred starting design: the Raft log is sole replicated durability and a
   future LSM apply path bypasses or makes its WAL explicitly non-sync. Keeping
   the synchronous data WAL is an alternative only with a proven double-log
   ordering/recovery protocol.
6. Phase 3 needs an API equivalent to
   `ApplyCommitted(index, term, logicalCommand)` that installs the supplied
   order without allocating it locally and publishes the applied-index marker
   atomically with the mutation.
7. Persisted applied index plus command identity makes replay idempotent:
   indexes at/below it are verified/skipped; the next index applies once.
8. Start by evaluating a portable logical snapshot: sorted latest logical
   keys/values/tombstones plus applied Raft index/term. A physical LSM snapshot
   remains a performance option after its file/Manifest ownership is proven.
9. Replicas must agree on committed command sequence, applied Raft index and
   latest logical key/value/tombstone state.
10. SSTable file numbers/boundaries, compaction schedule, Manifest generation,
    WAL file identity and cache contents may differ between replicas.
11. Before integration, Phase 3 must settle the atomic apply/applied-index
    contract, local-WAL mode and recovery ordering, snapshot representation,
    proposal waiter lifecycle, real transport/runtime ownership and the read
    protocol. Leader leases remain incompatible with the current no-clock-bound
    assumption unless that failure model is explicitly changed.

## Deferred scope and limitations

PreVote, CheckQuorum, leader transfer, dynamic membership/joint consensus,
chunked snapshot transport, real RPC transport, client deduplication, ReadIndex
and lease reads are deferred. The whole-file Raft store favors a small auditable
crash boundary over throughput and has a 256 MiB cap; segmentation is a future
Store replacement. The test state machine is in memory beyond snapshot bytes,
and there is no process-level network or integrated-database history checker.

## Certification record

The final Phase 2 commit was made only after `make certify-raft`, direct Go
1.25/1.27 vet/test/race compatibility checks, formatting/lint and the Phase 1
local certification passed. The final report records the command outcomes and
commit hash.
