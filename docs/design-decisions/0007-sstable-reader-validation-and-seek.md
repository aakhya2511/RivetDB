# ADR-0007: SSTable reader validation and seek strategy

**Status:** Accepted
**Date:** 2026-09-09
**Phase:** 1E

## Context

An SSTable index is both persistent input and the routing structure for Seek.
Checking its checksum, key ordering and handles is insufficient: a
rechecksummed or crafted boundary key can remain ordered while directing a
lookup past the block that contains its true lower bound. A production reader
must therefore prove every index key equals its data block's actual last key
before using the index for logarithmic lookup.

The reader must also bound memory, support concurrent immutable reads, and
surface later I/O or corruption rather than treating a successful open as a
permanent guarantee about mutable storage media.

## Options

### Trust checksummed index boundaries and validate data lazily

This makes Open proportional to index size and validates only blocks touched by
queries. It is fast for large cold tables, but an internally consistent corrupt
index can silently misroute Seek before the relevant block is ever accessed.

### Validate index boundaries incrementally during seeks

This preserves a cheap Open and can cache proofs over time. The first seek may
have to scan every preceding block to establish that no false boundary skipped
the answer, so lookup complexity and latency become history-dependent and the
implementation needs shared validation state.

### Stream-validate every data block during Open

This reads the entire table once but retains only the decoded index, metadata
and one data block at a time. It proves index mapping, strict local and
cross-block order, restart structure, checksums and redundant metadata before
the index is trusted. Later reads remain logarithmic and revalidate the block
they access, detecting corruption or I/O failure after Open.

## Decision

Use `os.File.ReadAt`. Open reads and validates the fixed footer, index and
metadata, keeps copied decoded index and metadata in memory, then streams every
data block in physical order. The stream proves exact index last-key mapping,
contiguous data coverage, entry ordering, restart semantics and all metadata
counts and bounds. Only one bounded data block is decoded at a time; the whole
file is never loaded.

Seek binary-searches the resident full-key index with
`storage.CompareInternal`, loads and validates the candidate block, binary
searches independently decodable restart keys, then scans one restart interval
to the first key not less than the target. Iterators retain only their current
decoded block. Returned entries are copies.

A Reader is safe for concurrent methods and independent iterators. Close is
idempotent and excludes active reads with a reader-local RWMutex. The Reader
must outlive iterators; after it closes, all new operations and subsequent
iterator advances return `ErrClosed` cleanly.

No Bloom query or data-block cache is added. A future Bloom check can precede
index search, and a future measured cache can sit beneath the same validated
block-loading boundary.

## Consequences

- Open is O(file bytes) I/O and O(index bytes + largest data block) transient
  memory rather than O(index bytes) I/O.
- Once Open succeeds, index-based seeks cannot silently skip an answer because
  of a false persistent boundary.
- Seek performs an O(block bytes) integrity pass, then O(log B + log R + I)
  search/decoding, where B is block count, R is restart count and I is the
  bounded restart interval scan. It does not reconstruct every block entry.
- Every later block access repeats envelope and checksum validation, so
  post-open corruption is surfaced.
- mmap, whole-file reads, shared block caching and probabilistic filtering are
  deferred without changing public lookup semantics.

## Revisit if

Measured open latency for very large tables is unacceptable. Any replacement
must preserve the proof that an unvalidated boundary cannot misroute a lookup;
a persisted authenticated index summary or a separately verified cache would
be acceptable, merely trusting CRC-valid boundary bytes would not.
