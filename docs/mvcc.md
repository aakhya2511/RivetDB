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

## 5. Phase 6 transaction interpretation

Phase 6 retains this committed-history contract and adds persistent
`TxnAbort(2)` and `Intent(3)` kinds without changing the trailer. Transaction
reads interpret selected intents through replicated record authority. A
committed intent is logically visible at its CT, an aborted intent falls through
to older history, and a pending intent conflicts. Physical resolution adds the
lower-sorting committed value/delete or abort marker at the same CT.

## 6. Phase 7 split composition

The parent image copies every logical MVCC tuple through bootstrap barrier S,
including historical values, tombstones and abort markers. Delta replay keeps
the original timestamp and is complete through an explicit parent Raft-index
frontier. The final proof partitions the parent's complete history through F by
user key and transfers an HLC floor to both children. `GetAt` and `ScanAt`
therefore retain visibility after cutover. No MVCC GC was introduced.

## 7. Phase 8 replica migration

The migration state image is a full copy for the same RangeID. It canonically
encodes every committed MVCC version, tombstone, intent, abort marker,
transaction/participant record, applied watermark and HLC floor. Target restore
is incremental and durable, and a logical digest—not physical SSTable layout—
proves equivalence through the promotion barrier. Migration adds no MVCC GC.

## 8. Phase 9 telemetry and split keys

Telemetry derives latest committed logical bytes and distinct current user
keys from canonical all-version export. Historical bytes remain a movement-cost
estimate. The planner never splits an encoded internal key and never drops or
rewrites a version; all state movement remains owned by the Phase 7/8 protocols.
