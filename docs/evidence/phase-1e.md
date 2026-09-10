# Phase 1E evidence

**Date:** 2026-09-09

**Result:** PASS

**Scope:** Production SSTable Open, exact Get, lower-bound Seek,
version-candidate lookup, full/target/user-range iteration, corruption-safe
decoding and concurrent immutable reads. No Bloom construction/query, flush,
manifest, VersionSet, compaction, cache, Raft or MVCC transaction behavior.

## Architecture and validation evidence

[ADR-0007](../design-decisions/0007-sstable-reader-validation-and-seek.md)
records the reader strategy. `Open` uses `os.File.ReadAt`, validates the fixed
footer, loads bounded copied index and metadata blocks, then streams every data
block while retaining only one at a time. The stream proves each full index
boundary, block-local and cross-block order, restart structure and all redundant
metadata before the sparse index is trusted. Index and metadata remain resident;
data blocks are reread, checksummed and decoded per operation. `ValidateAll`
rereads the footer and all blocks from the current file bytes.

The shared production decoder replaces Phase 1D's former test-only structural
implementation. Bounds and offset arithmetic are checked before conversion or
allocation. Block compression, kind and version are checked explicitly, CRC32C
is verified before payload decoding, ULEB128 encodings must be canonical, and
restart offsets must name increasing real entry boundaries whose entries have
zero shared prefix.

## Lookup and iteration evidence

Seek binary-searches the resident index for the first last-key boundary not
less than the target under `storage.CompareInternal`, binary-searches validated
restart keys, then scans the selected interval. Exact Get uses Seek plus the
same comparator. Candidate lookup constructs `(user key, target sequence,
deletion)` in one helper and preserves tombstones.

Six randomized datasets—fixed seeds `0`, `1`, `-1`, `8134472901`,
`-1234567890123` and fresh seed `3167509673348432449`—each contained 1,000
unique ordered entries and ran 5,000 reference lower-bound queries, 5,000
candidate queries and 500 half-open user-key range queries. Totals were 30,000
Seek, 30,000 candidate and 3,000 range comparisons. Full iteration was compared
directly to writer input without post-read sorting. Deterministic cases cover
before-first, after-last, exact block edges, targets between versions,
delete/value at equal sequence, binary/prefix keys, empty user key and one user
key spanning many blocks.

Iterators retain one decoded block, return copied entries, cross blocks without
duplicates or omissions, preserve every version and tombstone, and stay
exhausted after EOF. Ranges are decoded user-key `[start,end)` bounds, nil is
unbounded, equal bounds are empty and reversed bounds return `ErrInvalidRange`.

## Corruption and failure evidence

The production Open path is exercised by Phase 1D's every-offset truncation
test and rechecksummed structural mutations. Added reader cases cover
MaxUint64/zero/overlapping/out-of-region handles, index resource limits,
unterminated/overlong/noncanonical/overflowing varints, excessive shared
prefixes, reordered restarts and nonzero shared prefixes at restarts. A
systematic campaign flips every byte in a multi-block table; a separate seeded
campaign performs 1,000 random bit mutations. Full Open validation rejects all
of them. Mutation after a successful Open is detected on block access.

Injected seams cover underlying ReadAt errors, short reads, EOF and close
errors. Errors preserve major classifications through `errors.Is`, including
`ErrCorruptTable`, `ErrChecksum`, `ErrUnsupportedVersion`, `ErrInvalidHandle`,
`ErrInvalidBlock`, `ErrClosed`, `ErrNotFound` and `ErrResourceLimit`. The reader
never repairs, rewrites or skips immutable corruption.

Thirty-two goroutines concurrently mixed exact reads and independent iterators
while Close raced with them. Operations either completed or returned
`ErrClosed`; the race detector passed. Reader Close and iterator Close are
idempotent, and an iterator whose Reader has closed fails cleanly.

## Large-table evidence

The explicit stress run used seed `91734021`, 100,000 entries, four versions per
user key, varying binary values and tombstones. It opened and fully validated,
ran `ValidateAll`, exact full iteration, 1,031 sampled Seek/candidate pairs, a
50,000-entry user-key range and 800 concurrent exact reads:

```text
entries      100,000
data blocks  1,697
file bytes   6,946,722
sha256       94d7c16e9517fa2009b2eb94ecb4b6ff6dc1187beadb0a7811ff4e8f25e429af
```

## Benchmark baseline

Measured with Go 1.25.14 on darwin/arm64, Apple M4, `-benchmem
-benchtime=5x`. The representative table contains 20,000 entries. These are
engineering baselines, not performance claims:

| Operation | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Open (complete streaming validation) | 2,708,008 | 7,690,288 | 60,320 |
| Seek hit | 2,558 | 5,272 | 25 |
| Seek miss after table | 216.6 | 0 | 0 |
| Exact Get | 2,400 | 5,272 | 25 |
| GetCandidate | 5,983 | 5,291 | 27 |
| Full iteration | 2,850,783 | 7,668,240 | 60,042 |
| Range iteration | 1,236,542 | 3,971,164 | 40,417 |

The hit paths include a fresh `ReadAt` and CRC32C pass over the selected data
block. Miss-after-table terminates from the resident index. Full and range
iteration return copied entries and retain no cross-operation block cache.

## Repository gate

The completed source and documentation passed the pinned Go 1.25.14 `make
check` gate: formatting, vet, golangci-lint v2.6.1, normal tests and race tests
twice. Go 1.27.1 vet, normal tests and race tests twice also passed. Module
tidying and whitespace checks passed unchanged. The normal suite reported 257
passing test/subtest events; the large reader and writer stress gates passed
separately. The module retains zero external dependencies and no `go.sum`.
golangci-lint remains reported only under Go 1.25.14 because the pinned build
cannot consume Go 1.27 export data.
