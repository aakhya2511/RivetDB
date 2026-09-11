# Replicated MVCC and range-local snapshot design

## 1. Design gate

The audit compared Raft index, per-range counters and HLC timestamps. ADR-0017
selects a 48-bit Unix-millisecond plus 16-bit logical HLC because it keeps Raft
ordering and data-version time distinct while giving Phase 6 one comparable
cross-range domain.

`PHASE 5 MVCC DESIGN GATE: PASS`

The existing comparator and encoding remain unchanged: user key ascending,
timestamp descending through `BE64(^timestamp)`, then kind ascending. A new
persisted replicated-MVCC storage mode prevents old Phase 3 Raft-index keys from
being silently read as timestamps.

## 2. Timestamp authority

One range replica owns one mutable HLC. It uses an injected physical clock.
Leaders assign timestamps before proposal and encode them in canonical v2
PUT/DELETE commands; followers never substitute local time. Apply observes the
replicated timestamp. Before assigning after election, a leader observes every
timestamped entry in its durable local Raft history, including uncommitted
entries. Restart initializes from the durable Manifest MVCC maximum and durable
Raft history. Gaps are harmless; regression, reuse and overflow fail admission.

Raft index orders and proves application of commands. MVCC timestamp selects
logical visibility. A no-op advances only Raft progress. Volatile and durable
MVCC maxima are range scoped and never replace `DurableAppliedRaftIndex`.

## 3. Reads and snapshots

`GetAt(K,T)` returns the newest value version with timestamp `<= T`; a selected
tombstone returns not found. `ScanAt([start,end),T)` performs the same selection
once per user key and omits selected tombstones. Candidate lookup seeks directly
to `(K,T)` in every authoritative source.

A range snapshot captures one locally applied MVCC timestamp. `SnapshotAt(T)`
rejects `T` above the local applied watermark with `ErrReplicaBehind`. The
handle stores RangeID, descriptor generation and timestamp; Get/Scan validate
ownership and use only that timestamp. Close releases tracking, is idempotent,
and makes later operations return `ErrSnapshotClosed`. The oldest active
timestamp is observable only as future GC groundwork.

Snapshot timestamp zero represents the empty history before the first mutation.
There is no snapshot-too-old error because Phase 5 performs no version or
tombstone collection. Handles do not survive process restart, but the same
timestamp can be reopened and queried afterward.

## 4. Physical lifecycle and read boundary

Compaction preserves the exact multiset of internal entries. Consequently
historical state lives in current outputs after compaction and obsolete inputs
may be reclaimed without pinning a physical VersionSet for the snapshot's whole
lifetime. Each individual read retains the normal Engine read lifetime.

MVCC timestamp visibility is certified. Distributed linearizable read
freshness is not. A follower may serve an old timestamp at or below its applied
watermark, but returns `ErrReplicaBehind` above that watermark. A comparable
timestamp queried on several ranges is not an atomic cluster snapshot.

## 5. Deferred Phase 6 work

Phase 5 adds no intents, write transactions, conflict detection, transaction
records, coordinator, 2PC, isolation claim or cross-range atomicity. Existing
value/delete kinds remain sufficient for committed Phase 5 state; Phase 6 must
allocate and specify any intent/lock representation deliberately.

## 6. Phase 6 readiness audit

1. A transaction read timestamp should be an HLC value chosen through a
   transaction-level protocol that proves each participant can serve it.
2. A commit timestamp should be an HLC value strictly above the read timestamp
   and every participant-observed conflicting timestamp.
3. Phase 5 HLC values are totally comparable across ranges; comparability alone
   provides neither atomicity nor freshness.
4. Phase 6 must detect a committed or pending write to each written key after
   the transaction's read timestamp.
5. Intents should use an explicitly versioned internal record kind carrying
   transaction identity and provisional value, not overload committed values.
6. Resolution must be idempotent and driven by an authoritative transaction
   record outcome.
7. Prepare, commit and abort transitions must each be replicated through the
   owning range's Raft group; 2PC coordinates those replicated states.
8. A replicated transaction record, not coordinator memory or timeouts, must be
   recovery authority.
9. GC will need the minimum of active snapshot/transaction timestamps and an
   acknowledged protected timestamp before removing versions or tombstones.
10. Snapshot isolation is the recommended first target. Serializable behavior
    requires a separate checker and conflict design and is not implied here.
