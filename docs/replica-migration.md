# Online replica migration

## 1. Design gate and authority

`PHASE 8 REPLICA MIGRATION DESIGN GATE: PASS`

The twenty design questions resolve as follows. A user range's committed Raft
configuration is consensus membership authority; MetaRange is placement,
routing, allocation, and administration authority. MetaRange monotonically
allocates never-reused MigrationIDs and ReplicaIDs. A target is created with a
new ReplicaID in a ReplicaID-qualified directory and enters persistent Raft
configuration only as a learner. A bounded checksummed logical snapshot at a
committed/applied index S supplies initial state; normal Raft replication
supplies S+1 onward while traffic continues. A committed promotion barrier P,
`matchIndex >= P`, `lastApplied >= P`, HLC floor, provenance, and equal full
logical digests prove readiness.

Promotion uses `Stable old -> Joint(old,new) -> Stable new`; joint commitment
requires both old and new majorities. The target votes only after the joint
entry commits. The final Raft configuration commits before MetaRange publishes
the new placement. If the source leads, leadership transfers to a caught-up
final voter before final removal. The source stops voting, campaigning,
replication, and service once excluded and metadata-cut-over; physical deletion
waits for durable retirement and zero lifecycle references. Full MVCC history,
intents, participant records, transaction records, HLC and state-machine
metadata are in the logical snapshot. A replicated MigrationRecord plus Raft
configuration and target provenance are recovery authority; takeover increments
an epoch. MetaRange rejects a second same-range migration and any split/migrate
overlap, while unrelated ranges proceed independently.

## 2. Identity and record

RangeID is the unchanged logical state machine, ReplicaID is one durable and
never-reused membership incarnation, and NodeID is its physical host. Moving
`Replica103@Node3` to Node4 allocates `Replica104@Node4`; it never relocates
Replica103. Target storage is `ranges/<RangeID>/replica-<ReplicaID>/` and binds
MigrationID, RangeID, ReplicaID, snapshot index/term, and digest before use.

The authoritative record contains the identities, expected range/catalog
generations, epoch, state, S, P, learner progress, committed configuration
version, digest, and last error. Exact old/joint/final sets remain in the
authoritative range Raft configuration; recovery reconstructs its next command
from that configuration plus the record cursor. Its monotonic states are:

```text
PLANNED -> BOOTSTRAPPING -> LEARNER -> CATCHING_UP -> READY -> JOINT
        -> PROMOTED -> SOURCE_REMOVING -> COMMITTED -> SOURCE_RETIRED
```

`ABORTED` is legal only before Joint. Joint is the point of no return; recovery
finishes forward.

## 3. Snapshot and catch-up

The current leader chooses S only after it is committed and applied. The
portable image contains every MVCC tuple (including tombstones and intents),
transaction and participant records, applied/safe-read watermarks, HLC floor,
lifecycle/lineage state, and required state-machine metadata. Its digest is
over canonical logical content, not SSTable layout. Chunks have snapshot
identity, offset, per-chunk checksum, total size, and overall digest. Transfer
is synchronous/bounded. An identical duplicate is a no-op; a conflicting
duplicate fails. An incomplete staging file is either resumed after verified
prefix recovery or discarded and restarted; it is never authoritative.

After atomic target installation the learner uses ordinary InstallSnapshot and
AppendEntries. Leader change restarts from the durable record/provenance. A
slow target remains CATCHING_UP and never changes voter quorum.

## 4. Metadata, retirement, and deletion

After Stable new commits, MetaRange performs a CAS over RangeID, old descriptor
generation/set, MigrationID, config version, and catalog generation. Bounds and
RangeID do not change; descriptor and catalog generations increment. A crash
between final Raft config and metadata is reconciled forward from Raft. A crash
after metadata but before local retirement marks source RETIRED on recovery.

A durable tombstone prevents the old ReplicaID from reopening, voting,
campaigning, serving, or being auto-adopted. `ErrReplicaRemoved` is returned to
direct requests. Deletion eligibility requires final Raft exclusion, metadata
exclusion, durable tombstone, terminal migration record, and no live lifecycle
reference. Directory existence is never authority. Deletion is an idempotent
rename-to-retired/delete protocol; a crash during it cannot reactivate state.

## 5. Crash proof

| Durable point | Membership authority | Placement authority | Recovery |
|---|---|---|---|
| record / target directory / partial snapshot | stable old | old descriptor | resume or abort; target is orphan |
| target snapshot durable / learner added / catch-up | stable old plus learner | old descriptor | verify provenance, resume Raft catch-up |
| barrier committed / READY | stable old plus learner | old descriptor | recheck P/applied/digest, enter Joint |
| joint appended | stable old | old descriptor | discard uncommitted suffix or retry |
| joint committed | Joint old,new | old descriptor | finish forward under dual quorum |
| transfer started/completed | Joint old,new | old descriptor | retry transfer or verify new leader |
| final appended | Joint old,new | old descriptor | retry until dual-majority commit |
| final committed | stable new | old descriptor | Raft wins; complete metadata CAS |
| metadata committed | stable new | new descriptor | durably retire source |
| retired / deletion partial / deletion complete | stable new | new descriptor | never reopen old ID; finish safe cleanup |

Abrupt failure at every named hook therefore has one safe continuation. Before
Joint, old voters remain authoritative and abort is safe. From Joint onward no
rollback can create an alternative quorum; only forward completion is legal.
Coordinator memory, filesystem presence, and stale metadata never decide.

## 6. Concurrency and scope

Migration installs no transaction or user-write fence during bootstrap and
catch-up. Prepared intents and home records remain live. A leader-source move
has a bounded transfer window and does not claim zero downtime. Same-range
split/migration and multiple membership changes are rejected; operations on
different ranges may overlap. Phase 9 may select and submit `MoveReplica`, but
does not alter any migration state transition or authority rule. The system
still contains no range merge, MVCC/transaction GC, stronger
isolation, ReadIndex, leases, SQL, or AI operator.
