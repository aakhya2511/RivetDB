# ADR-0017: HLC timestamp authority and replicated MVCC snapshots

Status: Accepted
Date: 2026-09-11
Phase: 5

## Context

Phase 5 needs one deterministic timestamp in every replicated mutation, strict
non-regression across leaders and restart, efficient historical lookup through
the existing internal-key comparator, and a timestamp domain that Phase 6 can
compare across ranges. Raft indexes order one range but are independent across
ranges and include non-data entries. Existing Phase 3 directories encode Raft
indexes as internal-key sequences and cannot be silently reinterpreted.

## Options

1. **Raft index.** It is already durable, unique and ordered within a range,
   but has no cross-range meaning and conflates consensus progress with data
   visibility.
2. **Per-range logical counter.** It is simple and restart-safe, but values from
   different ranges cannot provide the comparable transaction timestamps Phase
   6 will require without a new timestamp layer.
3. **Hybrid logical clock (HLC).** A physical-millisecond component provides a
   common comparable domain and a logical component preserves monotonicity
   during ties, clock regression and observed remote time. It costs explicit
   overflow handling and durable-history observation.

## Decision

Use a canonical unsigned 64-bit HLC: the upper 48 bits are nonnegative Unix
milliseconds and the lower 16 bits are a logical counter. Unsigned integer
comparison is timestamp comparison. Timestamp zero is a valid historical read
before all writes but is never a mutation timestamp. Physical milliseconds are
bounded by `2^48-1`; logical overflow advances the synthetic physical component,
and exhaustion at the maximum timestamp fails write admission.

Each range replica owns mutable HLC state and may share only the injected
physical `clock.Clock`. A leader assigns a timestamp before proposing; the
canonical command carries it and every follower applies that exact value.
Before assignment, the replica observes all timestamped commands in its durable
local Raft history, including uncommitted entries. Apply also observes the
timestamp. Thus leader change and restart may create gaps but cannot reuse or
regress an observed authoritative value.

Raft index remains the range-local command order and durable replay frontier.
MVCC timestamp becomes the internal-key sequence and visibility order. A new
persisted `ModeReplicatedMVCC` and Manifest maximum-applied-MVCC field prevent
Phase 3 directories from being reinterpreted. Flush installs the table, Raft
frontier and durable MVCC maximum atomically; compaction changes neither
authority.

`GetAt` and `ScanAt` select the newest version at or below a timestamp.
Range-local read-only snapshots pin one immutable timestamp, not one physical
VersionSet. Every version and tombstone is preserved, so current compaction
outputs—not obsolete files—carry history. Phase 5 performs no MVCC GC.

## Consequences

Comparable HLC values do not make a multi-range snapshot atomic or a local
read fresh. A replica rejects timestamps above its applied MVCC watermark.
Snapshot handles are process-local; their timestamp remains queryable after
restart. Physical clocks need not be synchronized for monotonic safety, though
future external-consistency claims would need stronger assumptions.

Phase 6 can choose one transaction read timestamp and later commit timestamp
from this domain, but still must design timestamp coordination, intents,
write/write conflict detection, intent resolution, 2PC recovery and isolation.

## Revisit if

Phase 6 requires more than 16 logical events per physical millisecond without
synthetic advancement, needs uncertainty intervals/external consistency, or
introduces an oracle with materially stronger guarantees.
