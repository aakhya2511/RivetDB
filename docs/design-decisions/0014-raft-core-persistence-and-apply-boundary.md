# ADR-0014: Raft core persistence and state-machine boundary

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 2

## Context

Phase 1 exposes a correct local latest-state Engine, but its client write API
allocates replica-local storage sequences and appends a data WAL. Reusing that
API inside Raft would mix consensus ordering with node-local ordering and could
silently create two durability authorities. Raft protocol tests also need
complete control over time, persistence and network delivery.

## Options

### Goroutine-centric node with direct transport and disk calls

This resembles a deployed process and makes blocking APIs natural. It also
makes transitions depend on scheduler timing, scatters persistence-before-send
ordering across callbacks and weakens exact replay.

### Pure Ready/Advance core

The core emits persistence, send and apply batches and a runtime acknowledges
them. This gives maximal separation and batching flexibility, but the
persistence dependency graph is a substantial protocol of its own.

### Synchronous event-driven core with injected atomic store

Each Tick, RPC or proposal is processed serially. Required persistent state is
atomically saved before the method returns dependent messages. A deterministic
runtime/simulator owns clocks and transport. This is less throughput-oriented
than Ready/Advance but makes the Phase 2 durability claim direct and testable.

## Decision

Use the synchronous event-driven core. Keep the Raft `Store` independent from
the Phase 1 WAL and provide memory and crash-safe file implementations. Use
opaque logical command bytes and a synchronous injected state machine. Index
zero is the initial sentinel; snapshots replace it with an index/term boundary.
Static peers are immutable for Phase 2.

A new leader appends a no-op. Leaders commit only a current-term index by
quorum counting. Core proposal admission is not client success; success belongs
to a runtime waiter released after local apply. Persistence uncertainty makes a
node fatal before it can emit a dependent response.

Implement atomic snapshot payloads and InstallSnapshot in the deterministic
transport. Defer transfer chunking, PreVote, CheckQuorum, leader leases,
dynamic membership, joint consensus and leader transfer.

## Consequences

Protocol executions replay exactly and durability ordering is localized.
Saving the whole Raft state is intentionally simple rather than high
throughput; a segmented append log can replace the Store implementation without
rewriting consensus. The core is single-owner and must be serialized by any
future concurrent runtime.

Phase 3 must design a deterministic storage apply API and decide whether the
data WAL is bypassed, non-sync, or retained with explicit double-log ordering.
It must not call current `Engine.Put` independently on replicas.

## Revisit if

Measured Raft-store rewrite cost or runtime batching requires Ready/Advance, or
dynamic membership requires configuration state in snapshots and the log.
