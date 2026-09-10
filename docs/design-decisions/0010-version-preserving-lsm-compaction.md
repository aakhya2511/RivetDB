# ADR-0010: Version-preserving LSM compaction

**Status:** Accepted
**Date:** 2026-09-10
**Phase:** 1H

## Context

Overlapping L0 flush files make future reads inspect an unbounded number of
tables. Compaction must reorganize them without MVCC snapshot or garbage
collection rules, and without weakening Phase 1G Manifest authority.

## Decision

The deterministic picker triggers at a configurable L0 file count, seeds the
oldest L0 file, computes transitive user-key overlap closure across L0 and L1,
and produces an immutable plan tied to an immutable Version. Higher-level
heuristics and grandparent overlap are deferred.

Execution opens every input through the production reader and performs an
O(N log K), O(K) heap merge using `storage.CompareInternal`. Equal internal
keys retain their multiplicity; file number and iterator ordinal only make heap
ties deterministic. Every version and tombstone is preserved.

Outputs reuse the Phase 1D writer. A configurable logical-byte target starts a
new output only at a different user key, so a single oversized version group
stays intact. Published outputs are reopened and validated before installation.

The expensive merge runs outside the VersionSet lock. Installation rechecks
that every planned input remains live with unchanged metadata, then commits all
input deletions and output additions in one Manifest VersionEdit. Stale plans
cannot commit. The existing replay frontier is not changed or inferred from
output sequence bounds.

Input files become logically obsolete only after Manifest fsync and Version
publication. They are tracked but never physically deleted because older
immutable Versions may still reference them. Durable uninstalled outputs are
orphans and file-number gaps are acceptable.

## Consequences

Compaction is structural reorganization, not semantic GC. It may retain bytes
that future MVCC rules could remove, but it cannot lose visibility information.
Only one executor runs at a time; Manifest mutation remains serialized by the
Phase 1G store. Automatic scheduling, physical reclamation, final reads, Bloom
filters and caches remain later work.
