# Raft membership and joint consensus

## 1. Phase 7 assumptions being replaced

Before Phase 8, a Raft node received one immutable `Peers` slice at open,
reduced it to one integer quorum, rejected every message from outside that
slice, let every peer vote and campaign, and replicated only to that slice.
The Raft store persisted hard state, snapshot, and log, but no configuration.
Configuration entries did not exist. Multi-Raft derived message identity and
range startup solely from the published `RangeDescriptor`; its directories
were keyed only by RangeID. The replicated-range state machine deliberately
returned `ErrSnapshotDeferred`. Consequently a target absent from metadata
could neither receive Raft traffic nor recover an authoritative role, and a
metadata edit would have unsafely changed membership without a Raft decision.

## 2. Authoritative configuration

Every group has a canonical, versioned committed configuration:

```text
Stable { version, voters, learners }
Joint  { version, oldVoters, newVoters, learners }
```

Node and replica identities are distinct at the Multi-Raft boundary. The Phase
8 Raft core still keys a member by NodeID because one replica of a given range
per node is enforced; Multi-Raft validates the corresponding ReplicaID/NodeID
pair in its range envelope and descriptor. A configuration is stored atomically with Raft
hard state, log, and snapshot. `EntryConfig` carries its canonical encoding.
Only applying a committed configuration entry changes membership. Snapshots
also retain the committed configuration, so compaction cannot erase authority.

Learner addition keeps the stable voter set byte-for-byte unchanged. A learner
receives AppendEntries and InstallSnapshot and applies committed entries, but
does not grant votes, campaign, lead, or count toward commitment/election.

## 3. Transition and quorum

For old `{A,B,C}` and new `{A,B,D}` the only voter transition is:

```text
Stable old -> Joint(old,new) -> Stable new
```

A joint entry is admitted only from a stable configuration with the target
already a learner. Once Joint is committed, commitment and election require
`majority(old) AND majority(new)`. This is not `majority(union)` in general:
for old `{A,B,C}` and new `{C,D,E}`, acknowledgers `{A,B,C}` are three of five
but have only one vote in the new set, so they cannot commit. (For the common
single replacement in RF=3, the predicates happen to coincide.) The final
stable entry is committed under both majorities. Current-term commitment and
log-up-to-date voting remain required.

Only one configuration entry may be uncommitted and only one migration may be
active for a range. Removed and unknown ReplicaIDs cannot vote or inject log
traffic. A node excluded by a committed final configuration immediately steps
down and cannot campaign.

## 4. Leadership transfer

If the source is leader, it stops admitting new proposals, selects a caught-up
voter present in the final set, and issues controlled transfer. Final removal
is forbidden until that transferee is established leader. Failure leaves the
joint configuration intact and retryable; it never forces removal.

## 5. Persistence and recovery

The committed configuration and its monotonic version are saved before any
response that depends on it. Restart uses Raft state, never MetaRange, to learn
consensus membership. A snapshot install atomically installs state-machine
bytes and the snapshot's committed configuration. Delayed traffic from a
removed or aborted incarnation is rejected by membership and MigrationID/
epoch checks.
