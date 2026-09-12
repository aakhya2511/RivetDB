# ADR-0019: Online range splitting and replicated metadata authority

Status: Accepted
Date: 2026-09-11
Phase: 7

## Context

Static catalog files cannot safely decide online ownership changes. A split
must preserve all MVCC history and acknowledged writes while transactions,
leaders, coordinators and processes fail. It also needs durable identity and
lineage without combining the operation with replica membership change.

## Options

For metadata authority we considered independently published node catalogs, an
external control plane, and a dedicated replicated MetaRange. Local catalogs
are available but can diverge across crashes. An external authority is a valid
production architecture but adds an unimplemented dependency and failure
model. A statically located MetaRange uses the certified Raft boundary, avoids
circular user routing, and makes one command the atomic catalog cutover.

For child identity we considered retaining the parent ID for the left child and
allocating two new IDs. Retention reduces churn, but makes one RangeID denote
different historical bounds and complicates stale references. Two new IDs cost
two groups but preserve immutable parent identity and explicit lineage.

For active transactions we considered partitioning prepared state into children
and fencing plus draining. Transfer can reduce blocking but turns one old
participant into multiple identities and requires rewriting durable records.
Drain keeps Phase 6 authority intact: new create/prepare is fenced, existing
outcomes resolve, and the image contains no unresolved intent.

For state transfer we considered physical SSTable sharing and logical image plus
Raft delta replay. Sharing can be much faster but couples proof to file lifetime,
reference counting and compaction. Logical transfer is slower and duplicates
space, but preserves the logical contract independently of physical layout and
supports deterministic idempotent replay.

## Decision

Use a reserved, statically bootstrapped Raft MetaRange as the sole dynamic
catalog, allocator, SplitRecord and lineage authority. Allocate two new child
RangeIDs and one SplitID monotonically. Both children inherit the exact parent
replica set and start as independent SHADOW Raft/LSM groups.

Replicate a parent transaction fence, drain nonterminal transaction state, then
commit bootstrap barrier S. Copy every logical MVCC tuple through S. Continue
ordinary writes and replay committed parent indexes after S with original
timestamps and explicit idempotency identity. Each child tracks a contiguous
ParentReplayThrough including irrelevant/no-op indexes.

Commit final parent fence F, replay both children through F, validate logical
partition and inherited HLC floors, then execute one MetaRange generation CAS
that replaces parent with both children and records immutable lineage. Before F
the split can abort without changing ownership. After F it must finish forward.
Only committed metadata activates children; the parent is retired forever and
retains physical state plus terminal transaction status records.

## Consequences

User writes avoid MetaRange in the common path, and remain available through
copy/catch-up, but the final fence creates a bounded unavailability interval.
Transaction create/prepare involving the parent is unavailable for longer and
old unprepared transactions retry. Logical bootstrap temporarily duplicates
data and retains retired parent storage. These are accepted Phase 7 costs.

The design creates reusable transfer/provenance/frontier primitives for Phase 8
but does not implement placement, joint consensus or safe replica deletion.
Read freshness remains the Phase 5/6 local historical contract; splitting does
not add ReadIndex or clock-dependent leases.

## Revisit if

Measured split cost justifies physical immutable-table sharing after a safe
reference-count and compaction design, or if prepared-state transfer becomes
necessary enough to justify lineage-aware transaction participant rewriting.

