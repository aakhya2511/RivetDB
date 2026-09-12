# Architecture Decision Records

An ADR records a decision that was genuinely contested — where a competent
engineer could have chosen otherwise — together with the reasoning that settled
it. The value is in the alternatives section: a decision with no rejected
alternatives was not a decision.

An ADR is written *before* the code it governs, and it is not rewritten
afterwards. If a decision turns out to be wrong, a new ADR supersedes it and
says why, so the history of the design stays legible.

## Format

```text
# ADR-NNNN: Title

Status:   Proposed | Accepted | Superseded by ADR-MMMM
Date:     YYYY-MM-DD
Phase:    which phase this governs

## Context        what forced a decision
## Options        each with its real advantages, not strawmen
## Decision       what was chosen
## Consequences   what this costs, including what it forecloses
## Revisit if     the observation that should reopen this
```

## Index

| ADR | Title | Status | Phase |
|---|---|---|---|
| [0001](0001-lsm-tree-over-b-tree.md) | LSM tree over B+ tree for the storage engine | Accepted | 1 |
| [0002](0002-range-partitioning-over-hashing.md) | Range partitioning over consistent hashing | Accepted | 4 |
| [0003](0003-explicit-internal-key-comparator.md) | Explicit internal-key comparator | Accepted | 1 |
| [0004](0004-wal-integrity-and-tail-recovery.md) | WAL fragment integrity and explicit tail recovery | Accepted | 1 |
| [0005](0005-memtable-skip-list.md) | Skip list for the MemTable ordered structure | Accepted | 1C |
| [0006](0006-sstable-physical-format.md) | SSTable block, index, footer and publication format | Accepted | 1D |
| [0007](0007-sstable-reader-validation-and-seek.md) | SSTable reader validation and seek strategy | Accepted | 1E |
| [0008](0008-memtable-rotation-and-flush-lifecycle.md) | MemTable rotation and flush lifecycle | Accepted | 1F |
| [0009](0009-manifest-versionset-and-replay-frontier-authority.md) | Manifest, VersionSet and replay-frontier authority | Accepted | 1G |
| [0010](0010-version-preserving-lsm-compaction.md) | Version-preserving LSM compaction | Accepted | 1H |
| [0011](0011-integrated-local-lsm-read-write-semantics.md) | Integrated local LSM read/write semantics | Accepted | 1I |
| [0012](0012-crash-visibility-and-physical-reclamation.md) | Crash recovery, visibility publication and physical reclamation | Accepted | 1J |
| [0013](0013-evidence-driven-local-storage-performance.md) | Evidence-driven local-storage performance architecture | Accepted | 1K |
| [0014](0014-raft-core-persistence-and-apply-boundary.md) | Raft core persistence and state-machine boundary | Accepted | 2 |
| [0015](0015-raft-to-lsm-replicated-state-machine.md) | Raft-to-LSM replicated state-machine integration | Accepted | 3 |
| [0016](0016-multiraft-static-range-routing.md) | Multi-Raft hosting and static range routing | Accepted | 4 |
| [0017](0017-hlc-mvcc-timestamp-authority.md) | HLC timestamp authority and replicated MVCC snapshots | Accepted | 5 |
| [0018](0018-snapshot-isolation-transactions-and-2pc.md) | Snapshot Isolation transactions, replicated intents, and 2PC | Accepted | 6 |
| [0019](0019-online-range-splitting-and-replicated-metadata.md) | Online range splitting and replicated metadata authority | Accepted | 7 |

## Planned

These decisions are open. They are listed so that the gaps in the design are
visible rather than implicit; each is noted in
[architecture.md](../architecture.md) §14.

| Topic | Decided in |
|---|---|
| Read path: Raft read vs ReadIndex vs leader lease | Phase 3 |
| Automatic split key selection: size-based vs load-based | Phase 9 |
| Replica migration protocol: push vs pull snapshot transfer | Phase 8 |
