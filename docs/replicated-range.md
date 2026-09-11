# Design Note: Durable Replicated Range (Phase 3)

**Status:** Phase 3 integration design gate passed; implementation must follow
this contract before the phase can be certified.

**Related:** [ADR-0015](design-decisions/0015-raft-to-lsm-replicated-state-machine.md) ·
[raft.md](raft.md) · [storage-engine.md](storage-engine.md) ·
[invariants.md](invariants.md) (REPLICA-1 … REPLICA-14)

## 1. Boundary

Phase 3 implements one statically configured full-keyspace range replicated by
one Raft group. Each process may own multiple independent `Range` instances;
all state and directories are range scoped. There is no routing, Multi-Raft
scheduler, membership change, MVCC, transaction or distributed-read protocol.
`LocalGet` and `LocalScan` inspect locally applied state and may be stale.

## 2. Four load-bearing decisions

1. **Ordering:** every PUT/DELETE uses its Raft log index as the storage
   internal-key sequence. No-op indexes are valid gaps; no local allocator is
   consulted.
2. **Apply boundary:** `ApplyCommitted(index, term, encodedCommand)` validates
   a canonical command, enforces increasing application, installs exactly one
   mutation into the active MemTable and only then publishes volatile progress.
   It never calls standalone `Engine.Put`, `Delete` or `WriteBatch`.
3. **Durability:** the committed Raft log is the client durability authority.
   Replicated apply creates no data WAL and performs no second synchronous
   durability operation. Standalone mode retains the Phase 1 WAL unchanged.
4. **Recovery:** a distinct Manifest field records the inclusive
   `DurableAppliedRaftIndex`. It advances only in the same durable edit that
   installs a flushed generation covering the next contiguous applied-index
   interval. Restart discards unflushed state and reapplies only entries later
   re-established as committed above that frontier.

These decisions establish `PHASE 3 INTEGRATION DESIGN GATE: PASS`.

## 3. Persistent identity and layout

One replica uses:

```text
node-N/ranges/R/
  raft/     RAFTSTATE
  data/     CURRENT, MANIFEST-*, *.sst
```

The Manifest persists an engine mode: legacy/explicit standalone or replicated
state machine. A replicated directory must be created fresh and opened with the
same mode; mismatches fail and are never auto-converted. Raft and LSM file
namespaces remain separate.

## 4. Command format

Commands use bounded canonical binary v1 bytes:

```text
magic[4] = "RVCM"
version_u8 = 1
type_u8 = 1 PUT, 2 DELETE
reserved_u16 = 0
key_length_u32 LE
value_length_u32 LE
key[]
value[]
```

DELETE requires zero value length. Keys and values use the existing SSTable
bounds. Decoding checks the complete length before allocation, rejects unknown
versions/types/reserved bits/trailing bytes, and returns owned bytes. Leaders
validate before proposal and every replica validates again at apply.

## 5. Applied progress and generations

Raft `lastApplied` is volatile consensus progress. The replicated engine also
tracks volatile applied/published progress for its current execution. A
MemTable generation records explicit `firstAppliedIndex` and
`lastAppliedIndex`, including no-op gaps inferred only from Raft's ordered apply
context. Those fields are independent of the smallest/largest mutation sequence
stored in the table.

A nonempty frozen generation is written, fsynced, renamed, directory-fsynced,
validated and then Manifest-installed. Its installation edit includes its
coverage end only if coverage begins at `DurableAppliedRaftIndex+1`. FIFO flush
ordering makes later installed generations unable to jump a gap. The durable
frontier and new table become authoritative in the same Manifest record/fsync.
Compaction copies no coverage and asserts the frontier is unchanged.

No-op-only progress need not force a Manifest edit. Reprocessing a no-op after
restart has no logical effect. A later mutation generation may include preceding
no-op indexes in its explicit coverage.

## 6. Apply, visibility and replay

`ApplyCommitted` accepts one Raft command entry. The expected index is greater
than the current volatile progress; skipped indexes are known by the Raft caller
to be processed no-ops. Application uses one serialized admission token:

```text
validate command and index/term
→ wait for immutable capacity
→ apply WriteBatch{FirstSequence: raftIndex} directly to MemTable
→ attach explicit applied-index coverage to generation
→ publish visible/applied index
→ optionally rotate
```

There is no WAL append or sync. Reads capture only the published sequence, so a
mutation is invisible until its complete MemTable application. Reapplying an
index in the current execution is rejected; an index at/below the recovered
durable frontier returns an explicit already-applied result. Conflicting
same-index identities in volatile tracking are fatal. A malformed committed
command or storage apply uncertainty stops the replica without advancing.

## 7. Crash proof

- Before quorum: Raft never calls state-machine apply, so no LSM effect exists.
- Commit before apply: the quorum Raft log survives; commitment is learned
  again and the entry applies.
- MemTable apply/publication before flush: state is lost locally; the Manifest
  frontier is lower, so committed history reapplies.
- SSTable publication before Manifest: the table is an orphan; the frontier is
  lower and replay reconstructs the mutation.
- Manifest append/fsync: table plus contiguous frontier are one authority. On
  restart the table is live and entries at/below the frontier are not reapplied.
- Compaction: structural replacement neither changes ordering nor advances the
  replicated frontier.

Durable local log presence alone never authorizes apply because `commitIndex`
remains volatile. Only Raft commitment or an included Raft snapshot does.

## 8. Range runtime and proposal completion

A `Range` owns one RangeID, ReplicaID, Raft node/store, replicated-mode Engine,
apply coordinator and bounded proposal-waiter map. Core events remain
synchronous. A proposal is successful only after Raft commits it and the leader
locally applies it. Step-down resolves uncommitted waiters as leadership lost;
context cancellation removes only the waiter, never an admitted log entry;
shutdown resolves all waiters. A committed entry still applies after its client
leaves.

The deterministic integration network owns message queues, clock schedules and
crash/restart. No protocol logic moves into transport or timer goroutines.

## 9. Logical equivalence and snapshots

Replicas at the same applied Raft index must have identical ordered latest
key/value/tombstone state and digest. File numbers, Manifest generations,
MemTable/SSTable boundaries, compaction timing and caches may differ.

**LSM-INTEGRATED RAFT SNAPSHOT: DEFERRED.** Phase 3 does not expose Raft
snapshot/log compaction for integrated ranges, so no required committed history
is discarded. A logical replace-state snapshot is preferred, but safe atomic
directory replacement and per-key mutation indexes require a separate design.
Phase 2 snapshot behavior remains unchanged and certified.

## 10. Phase 4 boundary

RangeID, ReplicaID, Raft Store, Engine, directories, applied frontier and
waiters are per instance. No new global singleton is introduced. Phase 4 must
add range descriptors/routing, a node-level transport demultiplexer and shared
batched tick/runtime scheduling so hundreds of groups do not own one timer or
goroutine each.
