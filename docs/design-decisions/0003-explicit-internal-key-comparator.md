# ADR-0003: Explicit internal-key comparator

**Status:** Accepted
**Date:** 2026-09-09
**Phase:** 1

## Context

Phase 1 orders MemTable and SSTable entries by an internal key made of an
arbitrary binary user key, a sequence number and a value kind. Phase 5 will use
the same ordering for MVCC timestamps. Point lookup, seek, scan, compaction,
tombstone handling and later range splits all require every sorted component to
agree on one strict total order.

The original §5.2 representation was
`user_key || BE64(^sequence) || kind`, compared with `bytes.Compare`. The
complement correctly makes sequences descend only after equal-length equal
user-key bytes have already been established. It does not delimit a shorter
user key from its trailer.

## Problem and required properties

For distinct user keys, lexicographic user-key order must dominate for every
sequence and kind. For an equal user key, sequences sort descending; equal
sequences use a documented kind order. Comparison must be antisymmetric,
transitive, and equal only when all identity fields match. Consequently, every
version of one user key must remain contiguous.

The raw-byte design fails. Let `L` use sequence 0 and deletion kind, whose
trailer is `ff ff ff ff ff ff ff ff 00`; let `R` use maximum sequence and value
kind, whose trailer is `00 00 00 00 00 00 00 00 01`:

| User keys (`L < R`) | Encoded `L` prefix | Encoded `R` prefix | Raw result | Contract |
|---|---|---|---|---|
| `""`, `"a"` | `ff ff … 00` | `61 00 … 01` | `L > R` | fails |
| `"a"`, `"aa"` | `61 ff …` | `61 61 00 …` | `L > R` | fails |
| `"a"`, `"ab"` | `61 ff …` | `61 62 00 …` | `L > R` | fails |
| `"aa"`, `"ab"` | `61 61 ff …` | `61 62 00 …` | `L < R` | holds by differing user byte |
| `00`, `00 00` | `00 ff …` | `00 00 00 …` | `L > R` | fails |
| `01`, `01 ff` | `01 ff ff …` | `01 ff 00 …` | `L > R` | fails |
| `ff`, `ff 00` | `ff ff …` | `ff 00 00 …` | `L > R` | fails |

The first row is a minimal counterexample. Bytes `00`, `01`, `7f`, `80`, `fe`
and `ff` pose no special problem at a differing user-key position; every
prefix, including a prefix thousands of bytes long, has the same trailer-versus-
user-byte defect.

## Options

### Option A: explicit field-aware comparator

Keep the fixed-width-trailer representation, decode its boundary from the end,
and compare user key ascending, sequence descending, then kind ascending.

This has a short correctness argument and no encoded-key expansion. Every
MemTable ordering, SSTable block/index binary search, iterator merge,
compaction comparison and seek must call the same comparator. Prefix
compression remains byte-based because it only reconstructs keys; ordering is
still comparator-based. Bloom filters hash decoded user keys only.

### Option B: order-preserving self-delimiting user-key encoding

Encode every user byte into an order-preserving escaped pair and terminate the
user key with a byte that sorts before any encoded pair, then append the
trailer. For example, `(01, byte)` per input byte followed by `00` is correct:
the first differing input byte decides order, and a prefix terminator sorts
before the longer key's next pair.

This permits raw byte comparison, but roughly doubles user-key storage and
index/cache bandwidth, complicates encoded seek and boundary construction, and
makes dumps harder to read. More compact escaping schemes reduce average
expansion but make the proof and implementation more subtle. RivetDB has no
measured workload or subsystem that benefits enough from eliminating a small
field-aware comparator to pay those costs.

## Decision

Choose Option A. Logical fields are `(user key []byte, sequence uint64, kind
u8)`. Persisted bytes remain `user_key || BE64(^sequence) || kind`. Valid
sequences span the full `uint64` range. Valid kinds are deletion `0` and value
`1`. The only comparator is:

1. user key lexicographically ascending;
2. sequence numerically descending;
3. kind numerically ascending, so deletion precedes value.

Raw `bytes.Compare` on encoded internal keys is forbidden. The complemented
sequence is retained as an on-disk encoding detail, not relied upon to delimit
the user key.

## Consequences

- MemTables, data blocks, sparse indexes, binary searches, iterator merges and
  compaction all use the explicit comparator. The initial sparse index stores
  each block's full last internal key. Short separators require a separate
  user-key-level correctness proof before use.
- A snapshot seek constructs `(key, snapshot, deletion)` and takes the lower
  bound. The minimum kind includes either kind exactly at the snapshot.
  Versions remain contiguous, so scans can skip older versions by decoded user
  key and tombstone/version dropping can reason about a complete key group.
- Bloom filters contain user keys, not full internal keys. One membership test
  then covers every version visible at any snapshot and cannot reject an
  existing older version because its trailer differs.
- Range bounds and split keys are user keys. `[a,m)` and `[m,z)` partition
  logical keys; all versions follow their user key and cannot be divided by a
  sequence trailer. Shared immutable SSTables are filtered by those user-key
  bounds until compaction separates their contents.
- Comparator calls do more work than a raw byte comparison after a long equal
  user-key prefix. The representation has no expansion, and correctness is
  centralized and property-tested. Performance can be measured in Phase 1;
  it is not a reason to retain a disproved ordering.

## Revisit if

Profiling with the completed Phase 1 engine shows the comparator is a material
bottleneck and an alternative order-preserving encoding has a mechanical proof,
a migration plan, and measured end-to-end benefit that outweighs expansion and
complexity.
