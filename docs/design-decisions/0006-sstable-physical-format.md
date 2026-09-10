# ADR-0006: SSTable block, index, footer and publication format

**Status:** Accepted
**Date:** 2026-09-09
**Phase:** 1D

## Context

RivetDB needs an immutable table format whose writer can be proved correct
before a production reader depends on it. The prior design sketch specified
prefix-compressed data blocks and a 48-byte footer, but did not allocate a
metadata handle, authenticate block type/version, or checksum the footer. Those
omissions made structural corruption ambiguous and left future readers without
a mechanically complete layout contract.

The format must preserve ADR-0003 ordering, multiple versions, future MVCC
seeks, compaction, and temporary shared physical files during range splitting.
Partially written tables must never acquire a committed filename.

## Options

### Flat sequence plus footer

Encoding every full key/value consecutively is simple and easy to validate,
but wastes common prefixes and requires scanning the whole file to seek.

### Prefix-compressed blocks with shortened separators

Short separators reduce index size, but arbitrary-length versioned internal
keys make separator construction a new ordering proof. A subtly invalid
separator can direct a future seek to the wrong block.

### Prefix-compressed blocks with full last-key index entries

Restart points bound reconstruction work, full last keys preserve exact
comparator semantics, and independently typed/checksummed blocks localize
corruption. The index is larger than a shortened-separator index but has a
short correctness argument.

## Decision

Use format version 1. Every block is `payload || compression || kind || block
version || CRC32C`, where CRC covers the payload and all three trailer bytes.
Data blocks prefix-compress encoded internal keys and restart every 16 entries.
The first entry always restarts. The deterministic 4 KiB target counts the data
payload including restart offsets/count but excludes the seven-byte common
block trailer. An entry that fits legal resource limits may form one oversized
block.

Index blocks store each data block's full last encoded internal key and its
`(u64 offset, u64 length)` handle. Metadata stores counts, raw input bytes and
smallest/largest internal and user keys. Bloom construction is deferred; the
footer contains a zero filter handle and a clear filter-present flag. A filter
block can be added before the footer without changing version 1 handle layout.

The footer is fixed at 80 bytes at EOF. It stores index, metadata and optional
filter handles, the exclusive end of the data region, format version, flags, a
zero reserved field, a CRC32C over all footer bytes except the CRC field, and
the literal eight-byte `RIVETSST` magic. All fixed integers except the
internal-key sequence trailer are little-endian. Length varints are canonical
unsigned LEB128.

The writer rejects comparator-equal duplicates and out-of-order input; it never
sorts. Full index boundary keys are compared with `storage.CompareInternal`,
not encoded bytes. Prefix compression reconstructs bytes only.

Files use twelve-digit decimal identities (`000000000123.sst`). A writer
creates the deterministic `.sst.tmp` name exclusively, writes the complete
table, fsyncs and closes it, renames it to the final name, then fsyncs the
containing directory. The caller holds the database-directory lock across this
sequence. No manifest is changed in Phase 1D. Failures poison the
writer and leave ambiguous evidence in place unless the caller explicitly
aborts a still-temporary file.

## Consequences

- Phase 1E can locate the footer with one fixed-size read, validate it, then
  fetch typed index/metadata/data blocks independently.
- Full boundary keys make future internal-key lower-bound seeks correct even
  when versions of one user key cross a block boundary.
- Prefix compression improves common-prefix storage but requires reconstruction
  from a restart during reads.
- The footer and each readable block detect accidental corruption subject to
  CRC32C collision limits; semantic handle validation remains required.
- Files encode no owner range ID. Two logical child ranges may temporarily
  reference one immutable physical table and filter it by user-key bounds.
- Atomic filename publication is implemented, but manifest installation and
  WAL retirement remain later phases.

## Revisit if

Measured index size justifies a user-key-aware separator algorithm with a
mechanical comparator proof, or a future format needs compression/encryption.
Either change requires a new major version or an explicitly compatible block
version and Phase 1E validation support.
