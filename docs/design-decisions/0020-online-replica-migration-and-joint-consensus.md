# ADR-0020: Online replica migration with learners and joint consensus

Status: Accepted
Date: 2026-09-11
Phase: 8

## Context

Moving a live replica changes durable identity, state placement, Raft quorum,
and metadata at different times. Editing a peer list or copying a directory can
create disjoint quorums, promote incomplete state, or resurrect the old member.

## Options

Pure log replay has the smallest protocol but requires retaining and replaying
unbounded history. Raw SSTable transfer is fast and can reuse files, but binds
correctness to physical layout and compaction lifetime. A portable logical
snapshot plus Raft catch-up costs encoding and duplicate writes but has a clean
state-equivalence boundary. Single-step voter replacement is operationally
simple but unsafe under failure. Joint consensus is more stateful but gives a
mechanical intersecting-quorum proof.

## Decision

Allocate a fresh never-reused ReplicaID, durably bootstrap it as a non-voting
learner from a checksummed logical snapshot, catch it up with normal Raft,
prove readiness through a committed barrier and logical digest, then transition
through explicit joint consensus. Commit final Raft membership before the
MetaRange placement CAS. Transfer leadership before removing a source leader.
Persist a retirement tombstone before old data becomes deletion-eligible.

Raft configuration is consensus authority; MetaRange is allocation and
placement authority. Joint quorum is majority(old) AND majority(new). Snapshot
transfer is bounded and restartable. Joint is the point of no return.

## Consequences

Raft stores and snapshots carry configuration state; transports route learners
before descriptor publication; state-machine snapshots include transaction
metadata and full MVCC history; recovery must reconcile temporary Raft/metadata
disagreement forward. Phase 8 deliberately pays logical serialization cost and
does not implement automatic placement or zero-copy transfer.

## Revisit if

Measured migration cost justifies a physical-file optimization with equivalent
lifetime, integrity, portability, and crash proofs, or a future Raft protocol
defines a formally compatible safer membership primitive.

