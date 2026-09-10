# Phase 1D evidence

**Date:** 2026-09-09

**Result:** PASS

**Scope:** Version 1 SSTable physical format, ordered writer, structural
validation and atomic durable file publication. No production reader, seek,
iteration, Bloom lookup, flush, manifest, VersionSet or compaction.

## Format and writer evidence

[ADR-0006](../design-decisions/0006-sstable-physical-format.md) records the
format audit and decision. Data blocks prefix-compress encoded keys, restart
every 16 entries by default, and use a deterministic 4096-byte payload target.
All ordering validation calls `storage.CompareInternal`. Full last internal
keys index typed data-block handles; metadata records counts and internal/user
bounds; an 80-byte checksummed footer at EOF locates all structural blocks.
Every readable block authenticates payload, compression, kind and version with
CRC32C. Bloom construction is deliberately deferred with an absent filter
handle and clear flag.

The writer rejects exact duplicates, descending authoritative order, invalid
delete values and bounded-size violations. A successful `Finish` writes the
entire deterministic temporary file, fsyncs and closes it, renames to the
twelve-digit `.sst` identity, then fsyncs and closes the containing directory.
Injected write, sync, close, rename, directory-open/sync/close failures poison
the writer. Explicit abort removes only temporary state.

## Structural and corruption evidence

The validator exists only in `_test.go`; Phase 1E still owns the production
reader. It verifies:

- fixed footer size, magic, version, flags, reserved field and checksum;
- contiguous/non-overlapping index and metadata handles with no trailing bytes;
- common block envelope type/version/compression and CRC32C;
- canonical bounded entry lengths, key reconstruction and strict comparator
  order within and across data blocks;
- nonempty, increasing in-bounds restart offsets whose restart entries have
  zero shared prefixes;
- full index last keys, contiguous data handles and exact data-region coverage;
- entry/deletion/block/raw-byte counts plus internal-key and user-key bounds.

Targeted mutations cover data payload/trailer, index, metadata, footer checksum,
magic, supported-version field, footer handle overflow, block kind, noncanonical
varint, restart count/offset, index data handle and metadata count. Plain flips
fail checksum validation; internally rechecksummed malformed structures fail
semantic validation. Appending garbage fails. Every truncation offset of a
multi-block table fails validation without panic or recovery semantics.

## Ordering and deterministic evidence

Binary keys, empty keys, `00`/`ff`, long shared prefixes, same-user-key
versions, deletion/value distinctions, versions spanning block boundaries and
the ADR-0003 raw-byte prefix counterexample are covered. The writer rejects the
counterexample in authoritative descending input even though encoded-byte order
would accept it.

Two independent builds of a 2,000-entry table produced 50,223 identical bytes:

```text
sha256 d21f9ff9a01f448434cff2f71a11adc3e11081902f110584c6f321adab9d8266
```

Six randomized seeds (five fixed, one fresh) each generate 2,000 unique
internal keys, sort only with `storage.CompareInternal`, write many blocks and
compare every reconstructed key/value with the source sequence.

The explicit 100,000-entry stress run used seed `91734021`, included four
versions per user key, binary prefixes, varying values and tombstones, and
produced:

```text
data blocks  1,697
file bytes   6,946,722
sha256       088faf37de5a7ddc85eead4f7fdcaccb051ee6280ebcb532b1868539e068bf5f
```

Building that table twice produced byte-identical output.

## Benchmark baseline

Measured with Go 1.25.14 on darwin/arm64, Apple M4, `-benchmem
-benchtime=1x`. These are single-run engineering baselines, not performance
claims:

| Workload | Entries | ns/op | MB/s | B/op | allocs/op | table bytes | encoded/raw |
|---|---:|---:|---:|---:|---:|---:|---:|
| Sequential, 16-byte values | 100,000 | 10,831,125 | 304.68 | 17,825,400 | 402,273 | 3,005,667 | 0.9108 |
| Random ordered, 16-byte values | 100,000 | 13,767,625 | 297.80 | 32,261,408 | 403,246 | 4,341,943 | 1.0590 |
| Common-prefix-heavy, 16-byte values | 100,000 | 25,972,708 | 527.48 | 74,167,216 | 502,766 | 3,752,415 | 0.2739 |
| Sequential, 1-byte values | 100,000 | 8,636,542 | 208.42 | 11,789,032 | 401,160 | 1,487,959 | 0.8266 |
| Sequential, 4 KiB values | 10,000 | 22,643,916 | 1,816.38 | 185,452,280 | 70,092 | 41,660,192 | 1.0130 |

## Repository gate

The completed source and documentation passed the pinned Go 1.25.14 `make
check` gate: formatting, vet, golangci-lint v2.6.1, normal tests and race tests
twice. Go 1.27.1 vet, normal tests and race tests twice also passed. Module
tidying and whitespace checks passed unchanged. The module retains zero
external dependencies and no `go.sum`. Lint remains reported only under Go
1.25.14 because the pinned linter cannot consume Go 1.27 export data.

The normal suite reported 224 passing test/subtest events and two intentionally
skipped opt-in stress tests. The Phase 1D 100,000-entry stress test passed
separately with `RIVETDB_STRESS=1 RIVETDB_SEED=91734021`.
