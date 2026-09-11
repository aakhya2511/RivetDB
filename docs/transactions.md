# Snapshot Isolation distributed transactions

## 1. Design gate

The Phase 1–5 storage, Raft, replicated-range, routing and MVCC authorities were
audited before implementation. ADR-0018 settles identity, timestamp selection,
record placement, intent encoding, conflict detection, 2PC, epochs, recovery,
read interpretation, and the isolation boundary.

`PHASE 6 DISTRIBUTED TRANSACTION DESIGN GATE: PASS`

## 2. Isolation and timestamp contract

Every transaction has a random 128-bit TxnID and one immutable RT. Begin obtains
RT from an available range. Each range is required to replicate an RT serving
barrier before the transaction reads or prepares there. This proves a
consistent historical snapshot across touched ranges without coupling
unrelated range availability; it does not prove a linearizable or externally
consistent latest snapshot.

Reads select the newest committed version at or below RT and overlay buffered
writes. Commit chooses one CT strictly above RT and all participant HLC
observations. All writes use exactly CT. Prepare rejects committed writes newer
than RT and foreign active intents. These rules implement Snapshot Isolation
first-committer-wins. Read/write dependencies are not validated, so classic
write skew is deliberately allowed: Snapshot Isolation is not serializable.

## 3. Durable authorities

The record home is the replicated range owning the lexicographically smallest
written key. Its record contains TxnID, `PENDING|COMMITTED|ABORTED`, RT, fixed
CT, sorted participant `(RangeID,generation)` identities, home, and coordinator
epoch. The record is the sole outcome authority and is created `PENDING` before
prepare. Terminal states cannot change.

Each participant durably records TxnID, home, epoch, RT, CT, sorted writes and
its prepare/resolution state through its Raft log. Intents are LSM records at CT
with kind `Intent`; committed resolution adds `Value` or `Delete` at CT and
abort resolution adds `TxnAbort` at CT. Ordering is:

```text
user key ascending, timestamp descending, kind ascending
Delete(0) < Value(1) < TxnAbort(2) < Intent(3)
```

The existing `user || BE64(^timestamp) || kind` trailer is unchanged.

## 4. Protocol

```text
buffer writes
    -> replicate PENDING(record, RT, CT, epoch, participants)
    -> replicate PrepareTxn to every participant
       -> conflict: replicate ABORTED, resolve prepared participants
       -> all prepared: replicate COMMITTED(CT)
    -> resolve every participant according to the durable decision
    -> return the terminal status and fixed CT
```

Participants never infer commit from prepare. Resolution cannot precede the
matching durable decision. Duplicate create, prepare, decision, and resolution
operations are verified no-ops; inconsistent duplicates are protocol errors.
Commit success waits for resolution, though a committed decision remains
authoritative if the caller disconnects during cleanup.

## 5. Intent reads

The owning transaction reads its buffered overlay. A foreign committed intent
is logically a committed value at CT even before physical resolution. An
aborted intent is ignored in favor of older history. A pending intent that can
affect the requested timestamp returns a retryable conflict. If the record home
is unavailable, the reader blocks/retries; it never guesses. Ordinary Engine
reads cannot expose intent payloads as user values.

## 6. Recovery and fencing

Startup recovery reconstructs transaction records and prepared indexes from
committed Raft history. It resolves terminal decisions. For `PENDING`, recovery
first replicates an epoch increment, fencing the old coordinator, then records
abort and resolves participants. Timeouts can stop a wait or trigger recovery
but never create an outcome. A record-home quorum loss therefore blocks only
transactions dependent on that authority.

The recovery pass and participant work are bounded; no goroutine is created per
transaction and there is no process-global transaction execution lock.

## 7. Limits and future phases

Transactions have bounded write count, bytes, participants, and per-participant
command size. Terminal records are retained. No version, tombstone, or
transaction-record GC is performed; unresolved intents are never collected
without an authoritative decision.

Phase 7 split-readiness audit:

1. In-flight participants are currently the static `(RangeID,generation)`
   captured in the record; no descriptor may change during Phase 6.
2. A range with prepared intents cannot yet split. Phase 7 must order the split
   against prepare and durably divide each prepared write/index into the child
   that owns its key.
3. Transaction recovery needs a stable logical participant identity or a
   durable parent-to-child lineage; blindly retaining a removed RangeID is not
   sufficient.
4. Recovery must consult authoritative metadata redirects and fan out to every
   descendant holding a prepared key, while keeping one global record outcome.
5. Phase 7 must add generation pinning/admission, split/prepare serialization,
   participant redirect metadata, child prepared-state bootstrap, and crash
   tests at every metadata/data handoff.

Phase 8 must transfer record, participant and intent state during replica
migration without changing transaction authority. Neither split nor migration
is implemented in Phase 6.
