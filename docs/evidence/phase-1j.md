# Phase 1J evidence — crash recovery, reclamation safety and deep stress

Date: 2026-09-10

## Visibility authority and process-crash evidence

The pipeline now exposes separate monotonic `LastAssigned` and
`VisibleSequence` authorities. One write batch is assigned a contiguous
interval, crosses the `SyncBatch` WAL durability boundary, applies wholly to
the same active MemTable, publishes its final sequence once, and only then
acknowledges. Get and Scan capture `VisibleSequence`, never the assignment
counter.

`TestBatchVisibilityPublishesAtOneExplicitBoundary` proves the mechanical
pipeline boundary. `TestEngineBatchVisibilityUsesPublishedHighWater` pauses a
three-entry `Put(A), Put(B), Delete(C)` batch at each structured stage:
`WRITE_ASSIGNED`, `WAL_WRITTEN`, `WAL_DURABLE`, `MEMTABLE_APPLY_STARTED`,
`MEMTABLE_APPLY_COMPLETED`, and `VISIBILITY_PUBLISHED`. Concurrent Get and Scan
see all three old values through apply completion, then both see the complete
new batch at publication. No sleep or scheduler timing is used.

`TestSubprocessCrashBoundaries` runs five child processes and calls `os.Exit(86)`
without `Engine.Close`. Exit at assignment recovers the old state. Exit after
the complete WAL write, after WAL durability, after MemTable apply and after
publication recovers the full batch after process exit. The written-before-sync
case is not a machine-crash durability claim: complete page-cache bytes remain
visible after this process-only exit. The post-sync outcomes are
durable-but-client-ambiguous; the test does not incorrectly require an
unacknowledged durable write to disappear. Injected pipeline and WAL tests
separately fail the append and fsync boundaries, and exhaustive every-offset
truncation covers torn writes.

## Physical reclamation evidence

WAL maintenance is candidate-only. `InspectWALReclamation` decodes every
complete batch in the one current file, reports its sequence coverage and
marks the whole file eligible only when every batch ends at or below the
already-durable replay frontier. `TestWALReclamationCoverageIsCandidateOnly`
observes frontier 0 with a wholly covered WAL, then a mixed 0..1 WAL that is
not eligible, and rejects a batch 0..2 against frontier 1 as straddling.
Physical deletion is disabled: `000000000001.wal` is the live append target,
not a closed immutable segment. Inspection never changes the frontier.

Obsolete SSTable deletion is implemented using the Engine operation lock as
explicit lifetime tracking. Every public Get, Scan, Flush and Compact retains
the shared lock for its complete file-use lifetime; reclamation takes it
exclusively and rechecks that candidates are absent from the current Version.
`TestScanVersionLifetimeBlocksObsoleteTableReclamation` holds a Scan immediately
after it captures the old Version, compacts four L0 inputs into a new Version, and starts
reclamation. All four input files remain until the old Scan returns its value;
only then are they deleted. New reads and close/reopen return the same value.
The second maintenance pass deletes zero files.

`TestObsoleteTableDeletionFailureIsMaintenanceOnly` injects all four unlink
failures. The current value remains readable, the files are retained, and a
retry deletes all four without a logical VersionEdit. The concurrent engine
test mixes reclamation with writes, Gets, Scans, flush and compaction.
`TestCloseRacesWithObsoleteTableMaintenance` permits maintenance to complete or
return `ErrClosed`, then reopens with exact state.

Only compaction inputs proven obsolete in the current process are eligible.
After restart, that volatile proof is deliberately lost and unlisted files are
reported as orphans. Final invalid/corrupt orphan files are classified but
cannot influence Get or Scan. Temporary and orphan cleanup remain report-only;
their deletion-failure modes therefore cannot affect authority.

## Deep model, restart and tombstone evidence

The opt-in command

```bash
RIVETDB_PHASE1J_STRESS=1 go test -count=1 -run '^TestPhase1JDeepStress$' -v ./internal/storage/engine
```

first passed in 167.49 seconds using fixed seeds `101`, `9901`, `8134472901`
and the fresh seed `-6202586903419925495`. After separating the WAL write and
sync observation boundaries, the exact recorded schedule replayed and passed
in 306.80 seconds. Actual modeled counts in both runs were:

| Operation/effect | Count |
|---|---:|
| Logical operations | 50,000 |
| Put | 7,575 |
| Delete | 2,498 |
| Get | 22,499 |
| Scan | 17,428 |
| Flush | 100 |
| Successful compaction | 24 |
| Reclamation pass | 24 |
| Obsolete SSTables deleted | 122 |
| Close/open restart cycles | 120 |

Each restart checkpoint compares every modeled key, every modeled deletion, a
full ordered Scan and SHA-256 logical digest, then runs integrated structural
validation. Final digest was
`ec37e4e7eee4614f8479f9d8f1dc1bc074b8945452a40b76d3875cb4a472f41e`.
Final `NextSequence` was 10,073, matching the exact Put+Delete count;
`LastAssigned` and `VisibleSequence` were both 10,072. Final
`NextFileNumber` was 245 (highest allocated number 244). Monotonic authorities
and retired-file tracking observed zero sequence or file-number reuse.

`TestTombstonesNeverResurrectAcrossLifecycle` runs 64 keys through `Put(A) →
Flush → Put(B) → Flush → Delete → Flush → Compact → Restart → Reclaim →
Restart`. Every Get remains not-found and full Scan remains empty. Reclamation
after restart deletes zero because old inputs are conservatively reclassified
as orphans; retained raw A/B/tombstone history never influences latest state.
The Phase 1H exact-multiset tests continue to prove compaction does no version
pruning or tombstone garbage collection.

## Crash, corruption and authority matrix

- WAL: every-offset truncation returns the maximal valid prefix; protected-byte
  checksum mutations, malformed headers/fragments and rechecksummed batch
  corruption fail. Only a proven incomplete tail is repairable.
- Manifest: every-offset truncation applies only a valid durable prefix; middle
  corruption and checksum-valid semantic corruption fail. Pre-install outputs
  remain orphan/input-authoritative; post-fsync replacement recovers atomically.
- CURRENT: temporary write/fsync/close, rename, directory open/fsync/close and
  Manifest rewrite matrices retain a recoverable authority. A new integrated
  test proves two Opens of corrupt CURRENT return the same error without
  changing a byte in the directory.
- SSTable: every truncation, block/footer mutation, malformed handle/varint,
  checksum-valid semantic/restart corruption and metadata mismatch fails.
  Missing or corrupt Manifest-live tables prevent Engine Open.
- Orphans: valid or corrupt unlisted final SSTables and stale temporary files
  never participate in Get or Scan. They are retained and conservatively raise
  future file-number authority.

No authoritative corruption is repaired from a directory scan and no physical
file is promoted merely because it exists. File publication and CURRENT switch
continue to fsync contents, rename, fsync the containing directory, and only
then change authority.

## Repository gate

The exact `make check` gate passed with Go 1.25.14: format check, vet,
golangci-lint v2.6.1, normal tests, and `go test -race -count=2 ./...`. The
post-change exhaustive WAL package took 550.055 seconds under this race run.
Explicit Go 1.27.1 vet and normal tests passed. Its full count-two race command
passed every non-WAL package but the WAL package exceeded the command's
10-minute harness timeout under a 98%-full filesystem; there was no race or
assertion failure. The complete WAL package was immediately rerun with the
same Go 1.27.1 race/count-two settings and a 20-minute harness timeout and
passed in 503.961 seconds. `go mod tidy`, `git diff --check`, the no-`go.sum`
check and all phase-specific tests passed. The suite contains 237 top-level
tests and 402 named top-level/subtest cases.

The module retains zero external dependencies and no `go.sum`. Phase 1J added
no MVCC pruning, tombstone GC, transaction API, Raft/distributed behavior,
range operations, cache, Bloom filter, group commit or speculative performance
feature.
