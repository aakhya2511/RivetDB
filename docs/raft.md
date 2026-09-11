# Design Note: Raft Consensus Core (Phase 2)

**Status:** Implemented and mechanically qualified in Phase 2.

**Related:** [ADR-0014](design-decisions/0014-raft-core-persistence-and-apply-boundary.md) ·
[invariants.md](invariants.md) (RAFT-1 … RAFT-14) ·
[architecture.md](architecture.md) §4

## 1. Boundary

Phase 2 implements one static-membership Raft group against opaque logical
commands and an injected deterministic state machine. It does not import or
call the Phase 1 Engine, allocate storage sequences, or use the data WAL. The
Raft log is the only consensus durability authority in this phase.

The core is event driven. Its inputs are `Tick`, `Propose`, and one RPC; its
outputs are messages and ordered state-machine applications. It performs no
network I/O and reads no wall clock. A runtime or deterministic simulator owns
clock advancement, persistence availability, message delivery and proposal
completion. The core serializes each event synchronously.

## 2. State

Persistent state is one atomic value:

```text
HardState { currentTerm, votedFor }
Snapshot  { lastIncludedIndex, lastIncludedTerm, stateMachineBytes }
Log       { Entry{index, term, type, commandBytes}... }
```

Index zero is the initial sentinel with term zero. Real entries begin at one.
After compaction, the snapshot index/term is the retained logical sentinel and
stored entries begin at `snapshot.index + 1`.

Volatile state is `role`, `leaderID`, `commitIndex`, `lastApplied`, election
elapsed/timeout and votes received. Leaders additionally own `nextIndex` and
`matchIndex` per peer. Restart loads persistent state, becomes a follower with
unknown leader, and initializes commit/applied to the durable snapshot index.
Commit knowledge beyond the snapshot is recovered from a leader.

## 3. Storage and durability

`Store` loads and atomically saves the complete persistent value. Phase 2 has
an in-memory implementation and an independent file implementation. The file
store uses a bounded versioned binary snapshot with CRC32C and publishes it as:

```text
encode temporary → fsync temporary → close → rename → fsync directory
```

Term advancement and votes are saved before dependent RequestVote messages or
responses. A leader saves a proposal/no-op before replication. A follower
saves term/vote/log changes before a successful AppendEntries or
InstallSnapshot response. Any persistence uncertainty makes the node fatal; it
emits no safety-dependent response and takes no further part until restarted
from a known store.

The authoritative file is `RAFTSTATE`; `RAFTSTATE.tmp` is never authority. Its
byte-exact format is:

```text
magic[8]              = "RIVRAFT\x00"
version_u16 LE        = 1
reserved_u16          = 0
payload_length_u32 LE
payload:
  current_term_u64 LE
  voted_for_u64 LE    (0 means none)
  snapshot_index_u64 LE
  snapshot_term_u64 LE
  snapshot_length_u32 LE
  snapshot_bytes[]
  entry_count_u32 LE
  repeated entry:
    index_u64 LE
    term_u64 LE
    type_u8            (1=no-op, 2=command)
    reserved[3]        = 0
    command_length_u32 LE
    command_bytes[]
crc32c_u32 LE          over header and payload
```

The whole file is limited to 256 MiB, individual command/snapshot payloads to
16 MiB, and entry count to 4,000,000. Recovery rejects gaps, decreasing terms,
unknown types, nonzero reserved bytes, impossible lengths, unsupported version,
checksum failure, trailing bytes and a hard term behind its log/snapshot.

## 4. RPC model

Messages carry stable logical node IDs and one of RequestVote,
RequestVoteResponse, AppendEntries, AppendEntriesResponse, InstallSnapshot or
InstallSnapshotResponse. Append rejection includes the next index the leader
should try; a term conflict points to the first index of that conflicting term.
Duplicate, stale and reordered messages are safe. Simulator envelope IDs exist
only for deterministic scheduling and tracing.

## 5. Elections and time

Election and heartbeat durations are integral logical ticks. Configuration
provides minimum/maximum election ticks, heartbeat ticks and an explicit random
source. A production driver maps an injected `clock.Clock` ticker to `Tick`;
tests advance `clock.Mock` and choose which node ticks. Time affects liveness,
never safety. Receiving a current-term leader AppendEntries or granting a vote
resets the election timeout. Quorum is `n/2 + 1`.

A candidate increments term, votes durably for itself, and solicits votes.
Higher terms always cause a durable step-down. Log freshness compares last
term first, then last index. A new leader appends and durably stores a no-op in
its term before sending replication, allowing prior-term entries to become
committed only through a current-term entry.

PreVote, CheckQuorum, leader leases, leader transfer and dynamic membership are
deferred. They are not required for base Raft safety.

## 6. Replication, commit and apply

AppendEntries first proves the previous index/term. A follower removes only a
conflicting uncommitted suffix, appends a contiguous leader suffix, persists
the result, then reports success. A committed conflict is an invariant
violation. Followers advance commit only to
`min(leaderCommit, last index covered by this request)`.

A leader advances commit when a quorum has `matchIndex >= N` and entry `N` is
from its current term. Earlier entries commit transitively. Entries apply
synchronously, exactly once per node execution, in increasing index order and
only through `commitIndex`. Apply failure is fatal and never advances
`lastApplied` past the failed entry.

Core `Propose` means durable local admission, not client success. Runtime-level
proposal success is reached only when that index has committed and applied on
the proposing leader. Leadership loss may leave an admitted entry either
eventually committed or overwritten, so it cannot be reported successful
early.

## 7. Snapshots

Phase 2 supports atomic in-memory state-machine snapshots, InstallSnapshot and
log compaction. Snapshot metadata includes last included index/term and opaque
state-machine bytes. Creation is allowed only at `lastApplied`; the snapshot is
saved before covered log entries disappear. Installation rejects regression,
preserves a suffix only when its boundary term matches, persists the new
snapshot/log before restoring the state machine, and advances commit/applied
to at least the snapshot index. Transfer is one atomic simulator payload;
chunking belongs to a later real transport.

## 8. Deterministic qualification

The simulator owns directed connectivity, pending envelopes and stable message
IDs. It can deliver any pending message, drop, delay, duplicate, reorder,
partition asymmetrically, heal, tick, crash, restart and propose. Seeds come
from `testutil.Seed`; traces record bounded state and message digests.

Checkpoints enforce election safety, term/vote monotonicity, log matching,
leader completeness, leader append-only, quorum/current-term commit, committed
prefix preservation, apply ordering and state-machine safety across 3- and
5-node fixed/fresh campaigns. Durable-store tests separately inject save,
sync/publication and corruption failures.

## 9. Phase 3 boundary

Phase 3 must introduce a deterministic LSM apply API; `Engine.Put` and
`Engine.Delete` are unsuitable because they allocate node-local sequences and
append the data WAL. Raft log index is the recommended replicated mutation
order, with no-op/config entries consuming indexes but not storage mutations.
An applied-index marker must make replay idempotent.

The preferred integration candidate is Raft as replicated durability authority
with an LSM apply mode whose local WAL is bypassed or explicitly non-sync. The
alternative—Raft plus synchronous data WAL—reuses recovery but double-logs and
needs a proven ordering protocol. Phase 3 decides this before integration.

Replicas must agree on committed commands, applied Raft index and logical
key/value/tombstone state. File numbers, compaction timing, SSTable boundaries,
Manifest generations and cache contents may remain node-local. A portable
logical snapshot is the initial recommendation; physical snapshots remain an
optimization requiring tighter storage coupling.

## 10. Deferred scope

No LSM apply, data-WAL reuse, real network transport, dynamic membership,
joint consensus, ReadIndex, lease reads, client deduplication, Multi-Raft or
distributed database claim is part of Phase 2.
