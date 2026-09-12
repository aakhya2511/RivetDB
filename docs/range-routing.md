# Static range metadata and routing

## 1. Descriptor

A descriptor contains `RangeID`, monotonically increasing `Generation`,
`StartKey`, `EndKey`, and ordered replica records containing distinct
`ReplicaID` and `NodeID`. Bounds are logical user keys; internal-key trailers
never participate.

Endpoints use an explicit unbounded bit. In start position, unbounded means
negative infinity; in end position it means positive infinity. Therefore an
empty bounded key remains a real key. Ownership is exactly `[start,end)`.

Descriptors reject zero identity/generation, empty or duplicate replicas,
zero replica/node identity, duplicate nodes, invalid endpoint encodings, and
empty or reversed intervals. Phase 4 permits single-node descriptors only when
the bootstrap's expected replication factor is explicitly one.

## 2. Catalog

The default catalog is an immutable sorted descriptor array with unique IDs,
full `[-inf,+inf)` coverage, exact adjacency, and no overlap. Explicit partial
catalogs may omit coverage, in which case lookup returns `ErrRangeNotFound`.
`Lookup` uses binary search and is `O(log R)`; `LookupByID` uses an immutable
map. Snapshots deep-copy descriptor bytes and replica lists.

The persistent catalog is authoritative. Directory scanning only identifies
orphans and missing assigned storage; it never creates ownership.

The canonical little-endian v1 encoding is:

```text
magic "RVCT"       4 bytes
version             u16
flags               u16 (bit 0 = explicitly partial)
catalog generation  u64
replication factor  u32
node count           u32
range count          u32
node IDs             node count × u64
descriptors          range count × {
  RangeID            u64
  Generation         u64
  endpoint flags     u8 (start/end unbounded)
  reserved zero      3 bytes
  start length       u32
  end length         u32
  replica count      u32
  start/end bytes
  replicas           replica count × {ReplicaID u64, NodeID u64}
}
CRC32C               u32 over every preceding byte
```

The decoder caps the file at 32 MiB, nodes at 1,024, ranges at 4,096,
replicas/range at 31, and endpoints at the storage user-key limit. It rejects
non-canonical order, flags, reserved bytes, lengths, counts, checksum, trailing
bytes, truncation, and semantic layout errors.

## 3. Routed mutation contract

Routing performs:

```text
user key → immutable catalog lookup → (RangeID, Generation)
         → per-range leader hint → bounded proposal attempts
         → quorum commit + leader-local apply
```

The receiving Node verifies the route's RangeID, generation, local assignment,
and key ownership before Raft admission. The replicated state machine checks
key ownership again. A stale generation returns `ErrStaleRange` with the
current descriptor; a follower returns `ErrNotLeader` with a range-scoped
leader hint when known. Unknown ownership returns `ErrRangeNotFound`. Leader
hints never modify metadata and are keyed by RangeID.

Reads remain stale-capable local inspection. Routing a read to a leader would
not establish linearizability without ReadIndex or a proved lease, so Phase 4
makes no distributed-read consistency claim.

## 4. Phase 7 split routing

Phase 7 atomically replaces parent `[a,z)` with two new child RangeIDs
`[a,m)` and `[m,z)` in committed MetaRange state. A stale request to the
retired parent returns committed child descriptors. The router refreshes its
immutable metadata snapshot once, resolves lineage to the active leaf, and
retries there. SHADOW children reject direct traffic before cutover and the
retired parent never resumes user service.

Replica migration likewise retains RangeID while assigning a new ReplicaID to
a NodeID and physical directory. Phase 8 must design snapshot/state transfer,
catch-up, placement authority, joint membership transition, cutover, and
cleanup.
## 5. Phase 5 routed MVCC mutations

`PutMVCC` and `DeleteMVCC` retain the Phase 4 lookup, generation validation,
range-scoped leader hint and bounded pre-admission retry. The router never
chooses a timestamp: the selected range leader does so after leadership is
established. Historical reads remain range-local inspection; no ReadIndex,
leader lease, cluster snapshot or linearizable distributed read is introduced.

## 6. Phase 6 transaction routing

Transaction writes retain the descriptor generation resolved at commit and are
sorted by RangeID. The record home is the descriptor owning the smallest write
key. Protocol admission repeats generation and ownership checks; prepare also
validates every embedded write key. Status is routed to the record home rather
than read from coordinator memory.

Transactions continue to pin the descriptor generation used to build their
write set. If cutover makes that pin stale before prepare, commit aborts for
client retry rather than retargeting a transaction mid-protocol.
