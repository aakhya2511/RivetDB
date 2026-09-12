# Online transaction-safe range splitting

## 1. Boundary and authority

Phase 7 logically replaces one active parent `[a,z)` with two new child Raft
groups `[a,m)` and `[m,z)`. Both children inherit the exact parent replica set.
The parent is the only user-key authority until one committed MetaRange
`CommitSplit`; after that command the children are authoritative and the parent
is permanently retired. No intermediate catalog exposes both or neither.

The split worker is bounded orchestration, never authority. The replicated
MetaRange SplitRecord, parent split state, child bootstrap provenance and child
replay frontiers are sufficient for takeover after a crash.

## 2. State machine

Legal progress is:

```text
PREPARING -> COPYING -> CATCHING_UP -> READY -> FENCED -> COMMITTED
     |          |            |
     +----------+------------+----------> ABORTED
```

The abort edges exist only before FENCED. Every command carries SplitID and
epoch. Takeover increments the epoch before a new worker acts; stale workers
cannot advance parent, child, or metadata state.

## 3. Transaction fence and drain

`BeginSplit` installs a replicated parent transaction fence. The state machine,
not merely the router, rejects new `TxnCreate` and `TxnPrepare`. It continues to
accept terminal decision, takeover, and resolution commands. Ordinary
nontransactional Put/Delete/GetAt/ScanAt continue during PREPARING, COPYING and
CATCHING_UP.

Before the bootstrap barrier, the worker proves that the parent has no prepared
participant, unresolved intent, or nonterminal home record. Existing records
are resolved only from their replicated outcome; a pending home is epoch-taken
over and aborted by the Phase 6 recovery rule. Missing home quorum blocks the
split. A transaction holding only an old RT/buffered writes receives a stale or
split error at commit and aborts for client retry; its participant identities
are never silently rewritten.

Terminal records remain in the retired parent as a status archive. They are
not user-key authority and Phase 7 adds no record GC.

## 4. Bootstrap barrier and logical image

After drain, a parent Raft `BootstrapBarrier` commits at index S. The image is
an exact, stable logical enumeration through S of every internal MVCC tuple:

```text
(user key, timestamp, kind, value)
```

including old values, deletes, and abort markers. Intents are forbidden. All
versions of one key go to the same child by `key < splitKey`; equality belongs
to the right child. Child image digests hash this canonical ordering, not
physical SSTables.

Each new child is an independent Raft group and LSM. Canonical bootstrap chunks
are committed through child Raft and record SplitID, parent reference, S,
expected digest and inherited HLC floor. A child is ready only after every
assigned replica has durable image state, validated structure, matching digest,
and durable provenance. It remains SHADOW.

## 5. Delta replay

The retained parent Raft history after S is the bounded delta source. There is
no parent log compaction in the integrated range today, so source protection is
naturally satisfied. Only committed/applied entries are examined. A relevant
Put/Delete is replayed to exactly one child with its original timestamp and
with `(SplitID,parent index,command digest)` provenance. Transaction create or
prepare after S is a fatal split invariant violation.

Each child durably advances `ParentReplayThrough` for every examined parent
index, including no-ops and sibling-only commands. Repeating identical replay
is a verified no-op; conflicting identity is fatal. A sparse set of mutations
therefore never masquerades as prefix completeness.

## 6. Final fence and cutover

After catch-up approaches the parent head, `FinalizeSplitFence` commits at
parent index F. Admission racing it either commits before F and is replayed or
is rejected. After F the parent accepts no new user mutation, create, or
prepare. Both children must durably prove `ParentReplayThrough == F`, matching
partition digests, zero unresolved intents, and HLC floors at least the
inherited maximum.

`CommitSplit` is one MetaRange compare-and-swap over SplitID, epoch, parent
generation, and catalog generation. It validates the full candidate catalog,
replaces the active parent with both children, adds immutable lineage, and
increments catalog generation. This is the point of no return. If metadata
loses quorum after F, the parent remains fenced until commit can finish.

Committed metadata proof activates children. The parent then records RETIRED,
rejects user operations forever, returns descendant redirects, and retains its
physical LSM and terminal transaction archive. Parent deletion is not part of
Phase 7 correctness.

## 7. Crash authority table

| Hook | Catalog authority | User authority | Recovery | Abort? |
|---|---|---|---|---|
| META_SPLIT_RECORD_DURABLE | MetaRange old catalog | parent | takeover PREPARING | yes |
| PARENT_SPLIT_FENCE_ACTIVE | MetaRange old catalog | parent | drain transactions | yes |
| TXN_DRAIN_COMPLETE | MetaRange old catalog | parent | commit barrier S | yes |
| BOOTSTRAP_BARRIER_DURABLE | MetaRange old catalog | parent | regenerate image at S | yes |
| LEFT_IMAGE_DURABLE | MetaRange old catalog | parent | verify/resume right image | yes |
| RIGHT_IMAGE_DURABLE | MetaRange old catalog | parent | verify/resume replicas | yes |
| CHILD_QUORUM_READY | MetaRange old catalog | parent | resume replay | yes |
| DELTA_REPLAY_PROGRESS | MetaRange old catalog | parent | resume from child frontier | yes |
| FINAL_FENCE_DURABLE | MetaRange old catalog | fenced parent | finish forward replay | no |
| CHILDREN_REPLAYED_THROUGH_F | MetaRange old catalog | fenced parent | commit metadata CAS | no |
| META_CUTOVER_DURABLE | MetaRange new catalog | children | activate/retire forward | no |
| CHILD_ACTIVATED | MetaRange new catalog | active children | finish activation/retire | no |
| PARENT_RETIRED | MetaRange new catalog | active children | terminal verification | no |

Crashes within bulk copy, replay, post-fence/pre-cutover and post-cutover/pre-
retire are also exercised as abrupt subprocess exits. Full-cluster recovery
opens MetaRange first, never adopts directories, then resumes every nonterminal
SplitRecord.

## 8. Resource and scope limits

Image chunks, metadata records, active splits, replay batches, lineage depth,
and returned descendants are bounded. Workers stream from durable logical state
and Raft history; they do not retain an unbounded delta buffer or start a
goroutine per key. Different parents may progress independently; MetaRange
serializes only its small replicated edits.

This phase implements no range merge, migration, automatic placement or split
selection, user-range membership change, garbage collection, serializable
transactions, ReadIndex, or leases. Ordinary nontransactional traffic remains
available through bulk copy and catch-up; a short final write fence is required
for atomic cutover. No zero-downtime or linearizable-read claim is made.

## 9. Phase 8/9 readiness

The canonical image, provenance, chunk validation and durable catch-up frontier
can be reused to bootstrap a replica on a new node. Phase 8 must add placement
edits, ReplicaID rules, joint-consensus membership transition, learner/catch-up
semantics, deletion proof and transaction interaction. Split does not solve
those problems. Future rebalancing needs range bytes, read/write rates, apply
backlog, CPU, placement and split history; Phase 7 gathers no automatic policy.

Phase 8 implements those migration primitives without changing split
semantics. MetaRange rejects a split and migration on the same range, while
operations on different ranges may proceed independently. A split child is an
ordinary ACTIVE range and can subsequently migrate without changing lineage.
