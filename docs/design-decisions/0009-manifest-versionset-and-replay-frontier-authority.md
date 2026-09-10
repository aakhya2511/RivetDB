# ADR-0009: Manifest, VersionSet and replay-frontier authority

**Status:** Accepted
**Date:** 2026-09-10
**Phase:** 1G

## Context

Phase 1F can durably publish and validate an SSTable, but physical presence is
not logical membership. Recovery also needs durable authorities for file-number
allocation, sequence allocation and the WAL prefix represented by installed
tables. Inferring any of these by accepting every file in the directory would
promote crash orphans into live data.

## Options

### Directory scanning as authority

Scanning is simple and discovers collision hazards, but cannot distinguish a
completed flush from an SSTable whose manifest installation never became
durable. It remains useful only as conservative evidence for allocation and
orphan classification.

### One mutable metadata file

An in-place file is compact, but torn overwrite recovery needs a second
journaling scheme and makes atomic multi-field changes difficult to prove.

### Checksummed edit log plus immutable Versions

An append-only log makes the fsync boundary explicit. Candidate Versions can be
validated completely before the edit is appended and published only after the
append is durable. Periodic snapshot rewrite bounds future growth.

## Decision

The Manifest reuses Phase 1B's bounded 32 KiB block framing and CRC32C-protected
logical records. Its payload is a separate deterministic, versioned binary TLV
`VersionEdit`; unknown required fields fail and unknown explicitly optional
fields may be skipped. A structurally incomplete final record recovers the
complete prefix and is repaired before append. Corruption or semantic failure
at any complete record fails recovery and is never skipped.

`CURRENT` contains exactly `MANIFEST-%06d\n`. Creation and rewrite write and
fsync a new Manifest, write and fsync `CURRENT.tmp`, close it, rename it to
`CURRENT`, then fsync the directory. A directory-sync failure after rename is
ambiguous; neither Manifest is deleted and the open metadata writer is poisoned.
Explicit snapshot rewrite is implemented, with old Manifests retained.

The current `Version` is immutable. One mutex serializes candidate construction,
Manifest append/fsync and pointer publication. Readers receive deep copied or
immutable views. An edit is applied to a candidate in full; any invalid add,
delete, regression, duplicate, range or allocation leaves the current Version
unchanged. L0 overlaps and is ordered newest-first by sequence span, generation
and file number. Levels 1 and above are represented for Phase 1H and validated
as non-overlapping when populated; no compaction runs in this phase.
Structural operations are deliberately strict: adding an already-live file,
deleting an absent file, repeating either operation within an edit, and empty
edits are invalid. Repeating an equal scalar high-water mark is the one explicit
idempotent case; regressions remain invalid.

File numbers are reserved by durably advancing `NextFileNumber` before a number
is returned to a publisher. Recovery raises it above every live, orphan or
temporary SSTable number observed in the directory, and persists that
conservative advance. Sequence recovery takes the maximum sequence proven by
the Manifest, retained WAL and validated live SSTables; the next sequence is
one greater or explicitly exhausted.

The replay frontier is optional and inclusive: a complete WAL batch whose last
sequence is `<= frontier` is skipped. A batch whose first sequence is greater is
replayed; a batch straddling the frontier is rejected because replay must not
split an atomic batch. The frontier advances only in the same durable edit as,
or after, authoritative AddFile records and only when installed flush spans
cover every sequence after the previous frontier without a gap. Later physical
files beyond a gap do not advance it. The frontier is persisted but WAL files
are never deleted in Phase 1G.

A pipeline using Phase 1G obtains each file number from the durable allocator
before its first WAL write into that active generation. After Phase 1D
publication and Phase 1E validation, the worker calls the metadata installer.
Only successful Manifest fsync and Version publication produce the installed
terminal state. Installation failure retains the immutable generation and the
WAL; its final SSTable is an orphan candidate until recovery decides authority.

## Consequences

- A valid unlisted `.sst` remains an orphan and is never auto-promoted.
- Missing, corrupt or metadata-mismatched live tables fail Open.
- Allocation may intentionally leave gaps; safety is more important than dense
  numbering.
- Reusing WAL framing also reuses its conservative poisoned-writer and tail
  repair contract, while VersionEdit semantics remain independently versioned.
- Snapshot rewrite is explicit rather than policy-driven. Automatic thresholds,
  obsolete-file deletion and physical WAL reclamation remain later work.
- Phase 1H may emit AddFile/DeleteFile edits but must preserve Version atomicity
  and replay-frontier proof. Phase 1I will own final recovery/read integration.

## Revisit if

Manifest fsync cost requires group commit, automatic rewrite policy is measured,
or segmented WALs require a frontier richer than an inclusive sequence number.
