# ADR-0004: WAL fragment integrity and explicit tail recovery

**Status:** Accepted
**Date:** 2026-09-09
**Phase:** 1B

## Context

The Phase 1 WAL must recover the maximal complete logical-record prefix after a
crash while refusing to hide corruption. Its original seven-byte fragment
header stored one CRC over type and payload, followed by an unauthenticated
length. Recovery treated an incomplete or checksum-failing tail as truncation.

## Problem

An unauthenticated corrupted length can claim that its payload extends beyond
EOF. The payload checksum then cannot be computed, so those bytes look exactly
like a crash-truncated payload. Repairing that apparent tail could silently
discard later data. Separately, treating a final checksum mismatch as a crash
tail confuses a torn append with corruption of a previously complete record.

Recovery needs a local proof before trusting a length or mutating the file.
Checksums are probabilistic corruption detection; this decision assumes CRC32C
does not collide for the corruption under consideration.

## Options

### One content checksum and heuristic tail classification

Keep the seven-byte header. It is smaller and matches common WAL formats, but a
length that reaches EOF cannot be authenticated until bytes that do not exist
are read. Classifying based on position or plausible later bytes is a heuristic,
not a proof.

### Put one checksum after the payload

This detects complete-frame corruption but still cannot authenticate the
length before following it. It also adds a distinct crash point after payload
write and before checksum write without resolving the ambiguity.

### Independent header and content checksums

Use an eleven-byte header with CRC32C over length/type and another CRC32C over
length/type/payload. The reader authenticates the bounded physical length and
versioned type before reading or allocating for payload. This costs four bytes
per physical fragment and a CRC over three header bytes.

## Decision

Use independent header and content checksums. The format is:

```text
content_crc u32 LE
header_crc  u32 LE
length      u16 LE
type        u8      (high nibble version, low nibble fragment kind)
payload     length bytes
```

Version 0 defines FULL, FIRST, MIDDLE and LAST kinds 1 through 4. The logical
record limit is 64 MiB; every physical fragment is bounded by its 32 KiB block.

Only structural EOF incompleteness after an authenticated prefix is a
repairable tail. Every checksum mismatch and every semantic framing error is
corruption, even at EOF. Reading is non-mutating. Repair is explicit, rescans
the unchanged file, truncates to the last complete logical-record boundary and
fsyncs. It never acts on corruption.

## Consequences

- Header corruption cannot redirect parsing or cause a large allocation before
  detection.
- Exhaustive byte truncation has a deterministic outcome: complete logical
  records are returned; an incomplete logical record never is.
- A partially written header or payload is distinguishable from a checksum
  failure because successful writes preserve the exact encoded prefix.
- Recovery stops at corruption rather than resynchronising at a later block;
  preserving evidence and refusing an unproved history is more important than
  salvaging later records.
- Four extra bytes per fragment modestly increase small-record WAL overhead.
- A storage stack that loses already-fsynced bytes can still present structural
  truncation indistinguishable from an unacknowledged crash tail. RivetDB's
  durability contract assumes successful fsync is honored; violations of that
  filesystem/device contract are outside repair's proof.

## Revisit if

A future segment-level commit marker or authenticated log format can prove a
stronger durable boundary without adding equivalent per-fragment overhead, and
comes with crash-at-every-offset tests and a migration/versioning plan.
