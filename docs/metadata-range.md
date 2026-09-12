# Replicated metadata range

## 1. Design gate

Phase 7 introduces a reserved Raft group, `MetaRange`, whose peers come from
the static cluster bootstrap. It is located without consulting user range
metadata and is never split or migrated in this phase. Its replicated state is
the sole authority for active descriptors, catalog generation, monotonically
allocated RangeIDs and SplitIDs, split records, and immutable lineage.

`PHASE 7 ONLINE SPLIT DESIGN GATE: PASS`

The Phase 4 catalog file remains bootstrap input and a recoverable local cache.
Directories and cached catalog files never establish ownership after dynamic
metadata has been initialized.

## 2. State and commands

Metadata state is a bounded canonical binary value with an explicit version.
It contains the complete active catalog, `NextRangeID`, `NextSplitID`, split
records, and lineage. Decoding rejects truncation, trailing bytes, excessive
counts or lengths, invalid flags and states, duplicate identities, invalid
bounds, ID regression/reuse, lineage cycles, and catalogs with gaps or overlap.
It is not gob or JSON.

The replicated commands are:

- `BeginSplit(parent, generation, key, expectedCatalogGeneration)`, which
  atomically allocates one SplitID and two new RangeIDs and creates PREPARING;
- `AdvanceSplit(splitID, epoch, expectedState, evidence)`, which records only a
  legal monotonic transition and its durable evidence;
- `TakeoverSplit(splitID, expectedEpoch)`, which increments the worker epoch;
- `AbortSplit(splitID, epoch)`, legal only before FENCED;
- `CommitSplit(splitID, epoch, expectedCatalogGeneration)`, which validates
  proofs and atomically replaces the parent with both children, increments the
  catalog generation, and installs immutable lineage.

Command retry is idempotent by complete identity. A conflicting retry is an
error. Allocator gaps are retained; neither RangeIDs nor SplitIDs are reused.

## 3. Publication and routing cache

Each node and router may publish an immutable `CatalogSnapshot` read from a
committed MetaRange state. Publication is generation-monotonic. Requests name
RangeID and descriptor generation. A cache miss or stale parent response causes
one bounded refresh and reroute through committed active descendants.

MetaRange does not participate in ordinary reads or mutations. A user range
also checks its locally installed activation state: shadow children reject user
traffic even if contacted directly, and a retired parent returns its committed
lineage rather than serving user keys.

## 4. Validation

Candidate cutover is constructed in memory and passed through the complete
catalog validator before replication can apply it. The parent must be active at
the expected generation; child bounds must be `[parent.start,key)` and
`[key,parent.end)`; both children must inherit the exact ordered parent replica
set; IDs must be new and distinct; and the split key must be strictly interior
under the user-key byte comparator.

Lineage is append-only. Every retired reference has exactly one parent-to-two-
children edge. Validation prevents duplicate children, multiple parents,
self-edges, ancestor edges, cycles, and resolution beyond the configured depth
and result bounds. Resolution iteratively returns unique current active leaves.

## 5. Recovery authority

MetaRange Raft state, not a coordinator, supplies the recovery cursor. A new
worker first commits `TakeoverSplit`; commands from older epochs are rejected.
PREPARING, COPYING, CATCHING_UP, and READY may resume or abort. FENCED must
finish forward. COMMITTED is terminal and can only drive local activation and
parent retirement. ABORTED is terminal and leaves the catalog unchanged.

MetaRange corruption fails only that replica. Other metadata replicas may
retain quorum. No user-range directory scan is an authority fallback.

