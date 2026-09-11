# ADR-0018: Snapshot Isolation transactions, replicated intents, and 2PC

Status: Accepted
Date: 2026-09-11
Phase: 6

## Context

Phase 6 must atomically commit one write set across independently replicated
ranges. Coordinator memory, elapsed time, and participant-local intent state
cannot be outcome authority. Phase 5 supplies comparable HLC timestamps and
historical reads, but neither a cross-range serving guarantee nor distributed
atomicity. Static descriptors remain fixed until Phase 7.

## Options

For record placement we considered a dedicated system range, hashing TxnID into
the user keyspace, and colocating the record with a participant. A system range
centralizes load and adds a new availability domain; hashing needs a collision-
proof system-key namespace and can add a nonparticipant. The lexicographically
first write's range is deterministic, already participates, and keeps metadata
structurally inside that range's replicated state machine.

For provisional writes we considered installing intents at every `Put` and
buffering until commit. Early intents reduce commit-time work but complicate
ordinary reads, abandoned clients, and deadlock handling. Buffered writes make
read-your-writes a local overlay and install intents only after a durable record
exists.

For recovery we considered time-based presumed outcomes and a replicated
coordinator epoch. Time cannot prove an outcome and RivetDB assumes no clock-skew
bound. An epoch transition in the record can fence an old coordinator before a
recovery coordinator chooses abort.

## Decision

Implement Snapshot Isolation, not serializability. A transaction owns a random
16-byte TxnID and one immutable read timestamp RT. `Begin` obtains RT from an
available range leader. Before reading or preparing on another range, the
coordinator replicates RT as that range's serving barrier. Thus every touched
range proves it can serve the same timestamp without making untouched ranges an
availability dependency. This is a consistent available snapshot, not a claim
that the snapshot is a linearizable latest read. Transaction-local writes
overlay reads.

Writes remain buffered until commit. Participants and keys are sorted. The
record home is the range owning the lexicographically smallest written key. A
replicated `PENDING` record containing TxnID, RT, CT, epoch, home, and the final
sorted participant set is durable before any prepare. CT is fixed in that
record, is strictly greater than RT and every participant HLC observation, and
is identical on every participant.

Each participant replicates one atomic prepare containing its entire bounded
write subset. Prepare rejects a committed version newer than RT or a foreign
unresolved intent. Successful prepare installs durable intents at CT and records
the participant state. Only the replicated transaction record can change the
logical outcome to `COMMITTED` or `ABORTED`; both are terminal. Resolution is a
separate idempotent replicated operation and can lag the decision.

Persistent internal-key kinds are ordered `Delete(0)`, `Value(1)`,
`TxnAbort(2)`, `Intent(3)`. Existing values retain their bytes. At an equal CT,
the resolved committed kind sorts before the intent; an abort marker sorts
before the intent and tells reads to continue to older history. The internal
trailer and explicit comparator remain unchanged. An intent value canonically
encodes TxnID, home, epoch, RT, CT and value/delete payload.

Transaction-aware reads consult the authoritative record for a selected
intent: committed at CT is logically visible at/after CT, aborted is ignored,
and pending returns a retryable intent conflict. Ordinary storage reads never
return provisional bytes as committed values. Physical resolution only
materializes an already authoritative outcome.

Recovery enumerates replicated records and participant metadata reconstructed
from committed Raft history. A recovery coordinator first increments the
record epoch. It resolves terminal records and records `ABORTED` for a still-
`PENDING` record under the new epoch. Requests from older epochs are rejected.
Absence of a record is never interpreted as an outcome. Terminal records are
retained for retries; Phase 6 performs no version, tombstone, intent-without-
outcome, or transaction-record garbage collection.

## Consequences

The protocol uses four durable stages: create `PENDING`, prepare, decide, and
resolve. A single-range write transaction deliberately uses the same path.
Write/write conflicts provide first-committer-wins, while write skew remains
allowed; Snapshot Isolation is therefore not serializable. Loss of the record
home quorum blocks its transactions rather than inventing an outcome.

The lazy participant barrier is static-layout-specific. Phase 7
must pin descriptor generations, redirect participant identities through split
metadata, divide prepared state safely, and make recovery follow descendants.
Phase 8 must preserve record and intent state during replica migration.

## Revisit if

Measurements justify a one-phase single-range path, a dedicated transaction
range, early intents, parallel timestamp negotiation over only a declared read
set, or bounded record garbage collection with proved retry and intent safety.
