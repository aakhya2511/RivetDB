# ADR-0022: Compositional distributed chaos and reference-model certification

Status: Accepted
Date: 2026-09-12
Phase: 10

## Context

Phase-specific gates isolate failures well, but cannot establish that Raft,
MVCC, 2PC, splitting, migration and autonomous control preserve one another's
authorities under long overlapping schedules. Random goroutines plus a final
state check would miss transient safety failures and resist exact replay.

## Decision

Use two mandatory tiers. A deterministic logical model runs large event counts
and checks cheap global invariants every event plus canonical reference digests
periodically. A lower-count durable tier composes real FileStore, LSM, metadata,
split, migration, transaction and controller paths, including subprocess exits.

Every run has a versioned config, seeded PRNG, injected time, stable identity
order, exact replay command and bounded trace. The model authority is logical
state, not SSTable layout. Forbidden same-range operations are rejected;
permitted overlaps are explicit. Safety checks are distinct from post-heal
convergence and failures receive a safety/liveness/availability/harness/
environment classification.

## Consequences

Millions of dense events remain bounded while the durable tier still tests
publication and recovery. Passing evidence is replayable and checks transient
state, but does not prove unmodeled behavior, Byzantine tolerance,
linearizability, serializability or production liveness bounds.

## Rejected alternatives

- Only phase-specific tests are easier to diagnose but omit cross-layer states.
- Only filesystem chaos has fidelity but cannot afford dense schedules.
- Only an in-memory model cannot certify fsync, staging or process exits.
- Unseeded concurrent fuzzing makes scheduler failures unreliable to replay.
- Final-state-only checking misses temporary double authority and invalid quorum.

## Revisit if

A production transport or external history checker can preserve exact schedules
while exercising semantics absent from both current tiers.
