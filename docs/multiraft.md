# Phase 4 Multi-Raft design

## 1. Design gate

Phase 3 commit `96426135861644ced8c956d0046a4cf19cac3688` is certified and
the Phase 4 starting tree was clean.

`PHASE 4 MULTI-RAFT DESIGN GATE: PASS`

Two Phase 3 `Replica` objects can already coexist with independent Raft terms,
logs, snapshots, applied indexes, stores, Engines, Manifests, durable
frontiers, waiter maps, locks, and fatal states. Tick and Step are synchronous;
there is no hidden process timer, network goroutine, global log, global
Manifest, or global leader. These properties permit shared node-level services
without changing the Raft core or the Phase 3 durability boundary.

The Phase 4 composition is:

```text
Node
├── immutable RangeCatalog snapshot
├── concurrent RangeRegistry
├── shared bounded Transport
├── shared fair Scheduler
└── RangeID → Phase 3 Replica
              ├── Raft Node + Store
              ├── replicated Engine + Manifest frontier
              ├── proposal waiters
              └── range-local fatal state
```

Each envelope names one RangeID and carries a matching embedded group identity.
Each request carries RangeID and generation. Range storage and lifecycle remain
independent; scheduler or transport failure is node-wide by definition.

## 2. Static metadata and startup

The authoritative catalog is a bounded deterministic binary file beneath the
node's `cluster/` directory. It describes the complete cluster layout, not only
local replicas. Its checksum, format version, catalog generation, descriptor
generation, explicit bounds, and ReplicaID/NodeID pairs are validated before
publication or use. A local directory is never metadata authority.

Bootstrap validates the complete layout before publication. Publication is
temp file, contents fsync, close, rename, then containing-directory fsync. A
separate durable completion marker distinguishes retryable first-bootstrap
directory creation from a missing assigned directory after successful
initialization. Unknown directories are reported as retained orphans.

Startup loads the catalog, selects descriptors assigning this NodeID, opens
each range independently, registers successes, and records required failures.
A failed range does not prevent other registered ranges from operating; it is
never silently omitted from status.

## 3. Shared runtime

Raft remains deterministic and synchronous. The simulation scheduler owns a
stable list of `(NodeID, RangeID)` work keys and advances them round-robin.
Each work unit does bounded Tick or message delivery and then yields. The
physical Node runtime uses a fixed four-worker fair scheduler: per-range FIFO
queues permit at most one active item per range, so a blocked range consumes at
most one worker and cannot fill the pool with goroutines waiting on its lock.
The worker count and total pending work are fixed independently of range count.
One shared transport uses a bounded pending-message budget and fair range-aware
selection. There is no runtime goroutine or OS timer per range.

Per-range proposal waiter limits remain in Phase 3. Transport capacity and
maximum hosted ranges bound node-level memory. A slow/fatal range consumes at
most its work unit; other groups continue on later scheduler turns.

## 4. Failure domains

Range corruption, Raft persistence uncertainty, apply failure, restart, and
quorum are scoped to that RangeID. A process/node crash affects every local
replica. A shared transport or scheduler failure affects the node. The
simulator distinguishes node links from range-specific links and node crashes
from replica crashes.

## 5. Deferred work

Phase 4 does not implement dynamic metadata consensus. Future choices include
a dedicated meta-range, a distributed catalog, or an external placement
driver. It also does not integrate LSM Raft snapshots, online splitting,
migration, rebalancing, dynamic membership, MVCC, cross-range atomicity,
ReadIndex, or leader leases.
## 6. Phase 5 MVCC extension

Node construction may enable fresh MVCC ranges and inject a physical clock per
node. Every hosted replica still owns independent mutable HLC state. Routed
MVCC PUT/DELETE asks the owning leader to assign the timestamp and returns only
after the unchanged quorum-commit plus leader-local-apply boundary. Static
catalogs, generations, transport envelopes, scheduling and failure domains are
unchanged.
