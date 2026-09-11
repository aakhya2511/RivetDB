# ADR-0016: Multi-Raft hosting and static range routing

- **Status:** Accepted
- **Date:** 2026-09-11
- **Phase:** 4

## Context

Phase 3 owns one durable Raft-to-LSM replica. Phase 4 must let one physical
node host many such replicas without inventing a cluster-wide leader, log,
applied frontier, or storage engine. Ordered user keys also need durable
ownership metadata that can later change during a split without changing the
request contract.

## Options

### One process-global Raft runtime and one shared LSM

Prefixing keys and log messages with a range ID reduces file and object counts.
It also couples failure, compaction, recovery, and applied progress across
ranges. A corrupt range could stop unrelated ranges, and later movement would
have to extract physical state from a shared authority.

### One runtime stack per range

Giving every range its own transport goroutines and timers preserves isolation
and is operationally simple. Resource use scales directly with the number of
groups and permits a hot group to consume unbounded independent queues.

### Per-range state behind shared node services

Keep Raft, its Store, the replicated Engine, frontier, waiters, and fatal state
per range. Put only demultiplexing, bounded message queues, fair tick dispatch,
the immutable catalog, and leader hints at node/router scope. This requires
explicit RangeID-bearing envelopes and scheduling but preserves both lifecycle
isolation and bounded shared resources.

## Decision

Choose per-range replicated state behind shared node services. Persist a
versioned, checksummed, bounded static catalog using temp-write, file fsync,
rename, and directory fsync. Directories never confer ownership. Descriptors
contain RangeID, generation, explicit unbounded/user-key endpoints, and
distinct ReplicaID/NodeID pairs. The default catalog is gapless and
non-overlapping and lookup uses an immutable sorted array with binary search.

One Node opens exactly its catalog-assigned replicas under
`ranges/<RangeID>/{raft,data}`. A shared bounded transport carries an envelope
whose routing identity and embedded group identity must agree. A shared fair
scheduler drives synchronous Phase 3 Tick/Step methods without a timer or
goroutine per group. Range failures are retained and reported independently;
node-wide scheduler or transport failure is explicitly node-wide.

Routed operations carry RangeID and generation. The range checks generation
and key ownership before proposal, and its state machine checks key ownership
again before applying a committed command. Leader hints are per RangeID and
bounded retries do not change catalog authority.

## Consequences

There is one LSM and Raft Store per local range, so file counts grow with local
replica count. Static metadata is duplicated identically on participating
nodes; there is not yet a consensus metadata service. Startup can retry an
incomplete bootstrap, while corruption or a missing assigned directory after
the completion marker leaves that range explicitly failed. Unexpected range
directories are retained as orphans and never adopted.

No online split, merge, migration, placement, dynamic membership, distributed
read protocol, or cross-range atomicity follows from this decision.

## Revisit if

Measured group counts make per-range storage objects untenable, a replicated
meta-range is designed, or Phase 7/8 reveals an identity or atomic catalog
replacement requirement not represented by generation-bearing descriptors.
