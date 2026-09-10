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

## Planned

These decisions are open. They are listed so that the gaps in the design are
visible rather than implicit; each is noted in
[architecture.md](../architecture.md) §14.

| Topic | Decided in |
|---|---|
| Read path: Raft read vs ReadIndex vs leader lease | Phase 3 |
| Range metadata storage and client bootstrap | Phase 4 |
| Timestamp allocation: central oracle vs hybrid logical clocks | Phase 5 |
| Isolation level: snapshot isolation vs serializable | Phase 5 |
| Transaction recovery model | Phase 6 |
| Split key selection: size-based vs load-based | Phase 7 |
| Replica migration protocol: push vs pull snapshot transfer | Phase 8 |
