# Phase 1A/1B evidence

**Date:** 2026-09-09  
**Platform:** macOS, arm64, Apple M4  
**Scope:** storage primitives, write-batch codec, WAL framing/writer/reader,
recovery and explicit tail repair. No MemTable or later storage layer.

## Correctness gate

The repository gate passed with Go 1.25.14:

```text
make check
  gofmt check       pass
  go vet ./...      pass
  golangci-lint     pass, 0 issues
  go test ./...     pass
  go test -race -count=2 ./...  pass
```

Forward-compatibility validation passed with Go 1.27.1:

```text
go vet ./...                         pass
go test -count=1 ./...               pass
go test -race -count=2 ./...         pass
```

The pinned golangci-lint v2.6.1 was run only with Go 1.25.14 because its known
export-data decoder incompatibility prevents a valid Go 1.27 lint run.

Additional checks:

```text
go mod tidy       pass; no go.mod change and no go.sum
git diff --check  pass
test/subtest cases in one normal run: 149
external module dependencies: 0
```

## Crash and corruption evidence

`TestExhaustiveTruncationReturnsMaximalPrefix` constructs a 32,934-byte,
three-record, multi-block WAL and scans every cut in `[0, 32934]`: 32,935 crash
positions. Every cut returns exactly the complete logical records whose end
offset precedes the cut, never returns a partial record, reports the expected
valid-end offset and distinguishes a clean boundary from an incomplete tail.

`TestBitFlipInEveryProtectedByteStopsAtCorruption` flips one bit at every byte
of a complete middle record's two CRCs, length, type and payload. Every mutation
stops after the preceding valid record with `ErrCorruptWAL`; none is classified
as a repairable tail. Dedicated tests cover final-record checksum corruption,
authenticated impossible lengths, unknown versions/types, invalid fragment
sequences and nonzero block padding.

`TestRecoveryRepairAppendLifecycle` exercises:

```text
append A, B
sync A/B
append unsynced C
truncate C mid-payload
recover A/B and a tail token
refuse Writer open before repair
rescan and repair to valid end
append D/E with SyncBatch
reopen and recover A/B/D/E
```

Randomized reader tests passed with explicit seeds `7`, `8134472901` and
`-9223372036854775808`, in addition to built-in and fresh seeds. Those explicit
runs covered 150,000 arbitrary byte slices. The corresponding batch tests
covered 24,000 generated batches and the internal-key tests covered 180,000
generated comparator triples.

## Failure and lifecycle evidence

Tests inject short writes, partial-write errors, file sync failure,
containing-directory sync failure and close failure. A write or
sync failure poisons the writer; later Append/Sync calls cannot acknowledge a
healthy stream. Concurrent append testing uses 128 callers and verifies that
all records are complete, non-overlapping and recoverable under the race
detector. Repeated file-backed open/close cycles pass leak checks.

## Microbenchmark baseline

Command:

```text
go test -run '^$' -bench . -benchtime=200ms -count=3 ./internal/storage/wal
```

Median of three samples; short benchtime makes these orientation numbers, not
performance claims:

| Benchmark | Median |
|---|---:|
| Append 128 B, SyncNone | 2.060 µs/op, 62.13 MB/s |
| Append 128 B, SyncBatch | 3.649 ms/op, 0.04 MB/s |
| Append 4 KiB, SyncNone | 4.929 µs/op, 831.08 MB/s |
| Append 1 MiB, SyncNone | 456.493 µs/op, 2297.02 MB/s |
| Sequential read, 4 KiB records | 2.054 ms/op, 1994.39 MB/s |
| CRC32C, 128 B | 6.194 ns/op, 20665.16 MB/s |
| CRC32C, 4 KiB | 376.5 ns/op, 10880.37 MB/s |
| CRC32C, 1 MiB | 98.109 µs/op, 10687.90 MB/s |

No benchmark result is copied into project marketing or used as an acceptance
threshold.

## Remaining Phase 1 scope

Phase 1C and later still must implement and independently gate the MemTable,
SSTable, Bloom filter, manifest/version set, flush, compaction, complete engine
recovery, snapshots/iterators and engine-level benchmarks.
