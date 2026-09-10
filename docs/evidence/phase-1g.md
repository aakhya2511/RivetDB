# Phase 1G evidence — Manifest, VersionSet and durable installation

Date: 2026-09-10

## Scope and authority

Phase 1G adds `internal/storage/manifest` and the narrow Phase 1F pipeline
integration needed to distinguish physical SSTable durability from logical
installation. `CURRENT` selects one append-only Manifest; deterministic binary
VersionEdits replay into immutable copy-on-write Versions. A table is live only
after its AddFile edit has been fsynced. Directory discovery never promotes an
unlisted table.

The edit format records comparator identity, file additions/deletions, the
durable next-file and last-sequence high-water marks, and the inclusive WAL
replay frontier. L0 is ordered newest-first by largest sequence, flush
generation and file number. Higher-level metadata and non-overlap validation
exist for Phase 1H, but this phase executes no compaction.

## Crash and corruption evidence

`TestManifestCrashAtEveryOffsetAndMiddleCorruption` truncates a two-record
Manifest at every byte offset. Offsets before the complete initial snapshot
fail; later offsets recover exactly the maximal complete prefix. A bit flip in
a completed record fails checksum recovery.

`TestCurrentPublicationFailureMatrix` injects failures at CURRENT temporary
write, temporary fsync, temporary close, rename, directory open, directory
fsync and directory close. Pre-rename failures are unambiguous and removable;
post-rename failures are classified ambiguous. Successful publication executes
temporary-file write/fsync/close, rename and containing-directory fsync.

`TestCrashAfterManifestDurabilityRecoversLiveTable` stops after a successful
Manifest append/fsync but before volatile Version publication. The old in-memory
Version remains unchanged while restart recovers the new table as live.
`TestOrphansTempsMissingCorruptAndNumberRecovery` proves the opposite boundary:
an unlisted valid table remains an orphan, stale temporary files are classified,
and their numbers raise the durable allocation floor. Missing live files fail.
`TestCorruptAndMismatchedLiveTableFailRecovery` proves corrupt/truncated live
files fail rather than changing authority.

Manifest writer errors poison subsequent edits. Invalid edits are applied to a
candidate Version and never mutate an already-published Version. Explicit
rewrite writes and validates a one-record snapshot, publishes CURRENT with the
same crash-safe protocol, and retains the old Manifest.

## Model, concurrency and integration evidence

- `TestManifestReplayPropertyModel` compares recovered state with an independent
  in-memory transition sequence across 10,000 deterministic edits.
- `TestConcurrentVersionReadersSeeWholeSnapshots` races readers with 1,000
  installations; readers retain immutable complete Versions.
- `TestPipelineInstallAndReplayFrontier` runs WAL → MemTable → flush → durable
  SSTable → Manifest fsync → Version installation, reopens the metadata store,
  verifies next-sequence recovery, and proves WAL replay skips the installed
  prefix. The WAL remains physically present.
- `TestVersionCopyOnWriteOrderingAndAtomicRejection` covers deterministic L0
  ordering, old-Version lifetime, higher-level overlap rejection, and a replay
  frontier stopped by a gap.
- The strict CURRENT parser rejects missing newline, extra records, traversal,
  zero, and noncanonical Manifest names.

## Benchmark snapshot

One-iteration engineering smoke baseline on Apple M4, darwin/arm64, Go 1.25.14.
These values are not performance claims:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| VersionEdit encode, 100 files | 53,458 | 175,064 | 1,418 |
| VersionEdit decode, 100 files | 39,583 | 80,400 | 1,021 |
| Manifest append + fsync | 6,278,333 | 32 | 2 |
| Recover 100 edits (3,247-byte Manifest) | 137,166 | 31,664 | 411 |
| Recover 1,000 edits (32,047-byte Manifest) | 943,500 | 305,264 | 4,011 |
| Recover 10,000 edits (320,064-byte Manifest) | 9,627,916 | 3,041,616 | 40,019 |
| Explicit Manifest rewrite | 16,996,750 | 4,624 | 56 |
| Immutable Version install | 3,000 | 512 | 8 |
| Store install with physical SSTable validation | 63,833 | 3,456 | 77 |

Command:

```text
go test ./internal/storage/manifest -run '^$' -bench \
  'Benchmark(VersionEditCodec|ManifestAppendFsync|ManifestRecovery|ManifestRewrite|VersionInstall|DurableTableInstall)$' \
  -benchmem -benchtime=1x
```

## Gate

Both supported toolchains and the exact repository gate were run:

```text
Go 1.25.14: go test ./...
Go 1.27.1:  go test ./...
make check: fmt-check, vet, golangci-lint, test, race
```

No dependency was added. Phase 1G does not implement compaction execution,
the final multi-level read path, a table cache, Bloom filters, physical WAL
garbage collection, Raft, MVCC or distributed behavior.
