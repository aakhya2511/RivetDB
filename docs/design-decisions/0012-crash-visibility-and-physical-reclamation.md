# ADR-0012: Crash recovery, visibility publication and physical reclamation

Status: Accepted
Date: 2026-09-10
Phase: 1J

## Context

Phase 1I serialized the local write path, but the sequence used by readers was
derived from the next assignment. The surrounding mutex made this safe in the
existing implementation, yet it left no explicit distinction between an
assigned sequence and a completely applied, read-visible sequence. Phase 1J
also has to decide whether the single active WAL and compaction-obsolete
SSTables can be physically removed without weakening recovery or an old
Version reader.

A crash after WAL sync but before the caller receives success is different
from a rejected write: recovery may contain that durable batch. Physical file
existence is not authority; CURRENT's selected Manifest remains the only
logical table authority and its replay frontier is the only WAL redundancy
proof.

## Options

### Derive visibility from the next assigned sequence

This needs no second counter and was safe while one mutex enclosed assignment,
apply and read-snapshot capture. It couples correctness to that lock span,
however, and makes a future refactor capable of exposing an allocated or
partially applied batch.

### Publish an explicit visibility high-water

Keep `LastAssigned` and `LastPublished` authorities. Install a complete batch,
then atomically publish its final sequence, then acknowledge. This adds state
and test hooks but states the read contract directly.

### Delete WAL prefixes or the single WAL when below the frontier

Prefix rewriting could reclaim space now. The Phase 1 WAL has one open append
target, not closed immutable segments; truncating or unlinking it cannot meet a
whole-segment retirement proof and risks both the writer descriptor and later
batches.

### Compute WAL candidates but defer deletion

Decode complete batches, reject a frontier-straddling batch, and report whether
the whole current file is at or below the durable frontier. Keep physical
deletion disabled until WAL rotation produces closed immutable segments.

### Reference-count every immutable Version

Explicit leases permit prompt deletion as soon as the last old reader exits
and prepare for a table cache. They add a second lifetime system before Phase
1K establishes whether caching or finer-grained maintenance is needed.

### Use the Engine operation-lifetime lock

Every public read, scan, flush and compaction already holds the Engine read
lock for its complete file-use lifetime. Taking the exclusive lock waits for
all retained old Versions and in-flight users, then permits deletion of only
current-process compaction inputs absent from the current Version. This is
coarse and pauses admission during unlink, but its proof is small and explicit.

## Decision

The pipeline records separate assigned and published sequence authorities.
After validation and admission it assigns one contiguous sequence interval,
durably appends the encoded batch, applies the entire batch to the same active
MemTable, publishes the interval's last sequence once, and only then returns
success. Get and Scan capture the published value once for the operation.
Deterministic structured stages are `WRITE_ASSIGNED`, `WAL_WRITTEN`,
`WAL_DURABLE`, `MEMTABLE_APPLY_STARTED`, `MEMTABLE_APPLY_COMPLETED`, and
`VISIBILITY_PUBLISHED`. The pipeline opens the WAL in `SyncNone`, observes the
complete append, then explicitly calls `Sync`; acknowledgement semantics remain
equivalent to `SyncBatch`, while write and fsync failures are independently
observable.

A process exit at assignment, before any WAL write, loses the in-flight batch.
A process exit after the complete write may recover page-cache bytes, but this
is not a machine-crash durability guarantee. After WAL sync the batch must
recover even if MemTable application, publication or client acknowledgement did
not happen. An acknowledged batch must recover.

WAL reclamation is candidate-only. Coverage is computed at whole-file and
whole-batch granularity from the already-durable Manifest replay frontier.
Inspection never advances that frontier. Physical deletion remains disabled
because `000000000001.wal` is the live append target and Phase 1 has no closed
segment lifecycle.

Obsolete SSTable deletion is enabled conservatively. A maintenance pass takes
the exclusive Engine lock, rechecks that every candidate is absent from the
current Version, and deletes only inputs recorded obsolete by a successful
compaction in this process. The lock is explicit lifetime tracking: all public
read/scan/flush/compaction operations retain the shared lock while they can use
a Version or file. After restart, old unlisted inputs are orphans rather than
reclamation candidates because the volatile lifetime proof was intentionally
lost. Orphans and final invalid files are classified and retained; temporary
files are reported, not automatically removed.

Unlink failure reports maintenance debt and leaves logical authority unchanged.
Successful physical deletion creates no VersionEdit and needs no directory
fsync for correctness: failure to persist the deletion only leaves a harmless
extra non-authoritative file. Repeated maintenance is idempotent. This physical
file cleanup does not prune internal versions or tombstones.

The crash harness combines the production-path structured write stages, real
subprocess `os.Exit` tests, and the established WAL, SSTable, Manifest, CURRENT
and compaction injected-I/O matrices. Schedules are deterministic and seeded;
failing random campaigns print replay and seed-promotion commands.

## Consequences

Read correctness no longer depends on inferring publication from assignment.
The cost is two high-water fields and narrow observation hooks. Sequence values
assigned to a failed poisoned WAL append may be burned until restart; no
durable sequence is reused.

SSTable reclamation is safe but stop-the-world at the Engine API boundary and
cannot reclaim obsolete inputs discovered only after restart. WAL disk usage
remains unbounded in Phase 1. Those are deliberate space/performance costs in a
correctness phase, not hidden completeness claims.

## Revisit if

Revisit WAL deletion when WAL rotation creates closed segments with explicit
sequence coverage. Revisit Engine-wide exclusive reclamation when Phase 1K
profiles it or introduces a table cache; any replacement must retain explicit
Version/reader leases. Revisit orphan cleanup only when persistent publication
metadata can distinguish proven obsolete files from ambiguous outputs.
