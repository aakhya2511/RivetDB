# Phase 8 evidence

## Scope and design gate

`PHASE 8 REPLICA MIGRATION DESIGN GATE: PASS`

The proof and fixed-membership audit are in
[replica-migration.md](../replica-migration.md),
[raft-membership.md](../raft-membership.md), and
[ADR-0020](../design-decisions/0020-online-replica-migration-and-joint-consensus.md).
Raft committed configuration is membership authority; MetaRange is allocation,
placement and recovery-work authority.

## Deterministic and failure evidence

- New target ReplicaIDs are monotonic and never relocate/reuse the retired ID;
  three ordinary moves, a move back to the old node, and 20 opt-in disk-backed
  moves pass.
- Learners cannot vote or campaign, are excluded from quorum, and do not serve
  user traffic. Stable/learner/joint/final configuration transitions are
  canonical and versioned.
- The target installs a complete logical image containing MVCC history,
  tombstones, prepared intents, TxnRecords, participant records, HLC and apply
  metadata; source/target logical digests match through promotion barrier P.
- A 100,000-event membership model checks learner exclusion, dual-majority
  joint quorum and monotonically allocated replica identities. The explicit
  general counterexample is old `{A,B,C}`, new `{C,D,E}`, ACK `{A,B,C}`:
  three-of-five union majority exists but new has only one vote, so commit is
  rejected.
- Writes commit at record/learner/joint stages and are visible on the final
  target. A cross-range bank transfer commits while one participant range is
  migrating and preserves the total. Prepared participant and home-record
  migration, historical digests, leader-source transfer, election while joint,
  Phase-7-child lineage, same-range exclusion and different-range records pass.
- A 1,000-message retired-identity storm cannot change term, membership or
  commit index. Cleanup for an older identity is refused if a newer incarnation
  now occupies the node.
- Real source deletion followed by a full restart with authoritative MetaRange
  catalog opens the target and does not reopen the source.

The subprocess matrix uses actual `os.Exit(88)` at partial snapshot transfer,
target snapshot durable, joint configuration committed, final configuration
committed before metadata, metadata committed before local retirement, and
source deletion started. Each persisted root is reopened and recovered forward
to `SOURCE_RETIRED`; deletion-start is then completed.

## Verification commands

The following Phase 8-specific runs passed on Go 1.25.14/darwin-arm64:

```text
make check
make migration-race
make migration-stress
make migration-chaos
make migration-crash
```

Go 1.27.1 `go vet ./...` also passed. Final composed certification is
`make certify-migration`, which includes the unchanged Phase 7 through Phase 1
tiers; its result and exact commit are recorded in the certification handoff.

## Constrained-environment engineering baseline

This is a `CONSTRAINED-ENVIRONMENT BASELINE`, not a representative disk claim:

```text
filesystem: APFS (/dev/disk3s5)
capacity/free/utilization: 228 GiB / 6.6 GiB / 97%
benchmark/test root: /private/tmp/rivet-phase8-tmp
Go cache: /private/tmp/rivet-phase8-go-cache
hardware: Apple M4, darwin/arm64
OS: macOS 15.7.4 (24G517)
Go: 1.25.14
benchmark: BenchmarkReplicaMigration, -benchtime=1x
total: 827,445,958 ns/op
snapshot: 156 bytes; durable milestone 345,456,541 ns
catch-up: 2 entries; ready milestone 432,537,458 ns
joint commit milestone: 525,469,458 ns
leadership transfer milestone: 654,455,333 ns
MetaRange cutover milestone: 784,508,875 ns
allocations: 1,648,512 B/op; 10,745 allocs/op
```

Milestones are elapsed from benchmark start and therefore overlap; they are not
additive phase durations. No final fsync/flush/compaction or production
performance conclusion is drawn from this nearly full filesystem.
