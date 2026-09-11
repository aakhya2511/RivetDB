# ADR-0015: Raft-to-LSM replicated state-machine integration

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 3

## Context

Phase 1's Engine owns local ordering and durability through a sequence allocator
and synchronous data WAL. Phase 2's Raft owns replicated ordering and durability.
Calling the former client API from the latter would create two authorities and
replica-local order. Crash recovery also needs to distinguish volatile applied
state from state represented by an authoritative SSTable and Manifest.

## Options

### Raft log plus synchronous data WAL

This reuses Phase 1 recovery unchanged and makes each applied mutation locally
durable immediately. It double-logs and double-fsyncs, introduces an ordering
problem between two authorities, and makes a local WAL failure capable of
blocking an otherwise durable committed command.

### Raft authority with a non-authoritative local WAL

A non-sync WAL could accelerate reconstruction. It still needs replay identity,
truncation and applied-frontier rules and provides no Phase 3 correctness
benefit over direct reapplication from the retained Raft log.

### Raft authority with WAL-free materialized LSM state

Committed Raft entries apply directly at their log indexes. Flush installation
atomically advances a separate durable applied frontier. Unflushed state is
reconstructed from committed Raft history. This adds an explicit engine mode
and apply path but leaves one durability authority.

## Decision

Choose WAL-free replicated materialization. Persist standalone versus
replicated mode in the Manifest; never auto-convert directories. Use Raft index
as internal-key sequence and one mutation per command. No-op gaps are valid.
Introduce canonical bounded PUT/DELETE command bytes and a deterministic
`ApplyCommitted` path that bypasses standalone Put/Delete/WriteBatch.

Track applied-index coverage explicitly on each MemTable generation. The
Manifest atomically installs its validated SSTable and advances a distinct
contiguous `DurableAppliedRaftIndex`; never infer it from table sequence min/max.
Compaction cannot advance the frontier. Client success is quorum commit plus
leader-local apply. Proposal waiters are range-owned and bounded.

Defer integrated snapshots. Integrated ranges retain complete Raft history and
do not call log compaction until logical replace-state restore is designed.

## Consequences

Standalone Phase 1 semantics remain byte-for-byte on their existing path.
Replicated apply has no second WAL/fsync, and different replicas may flush and
compact independently. Restart may replay many committed Raft entries, and the
Phase 2 whole-state Raft store may grow to its limit; these are explicit Phase
3 costs.

The first implementation is one static full-keyspace range with a deterministic
in-process runtime. Range identity and all resources are instance scoped so
Phase 4 can host many groups behind shared transport and tick scheduling.

## Revisit if

Measured restart cost justifies a non-authoritative WAL, integrated logical
snapshots are ready, or segmented Raft storage changes retained-history costs.
