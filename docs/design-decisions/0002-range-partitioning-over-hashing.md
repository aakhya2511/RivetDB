# ADR-0002: Range partitioning over consistent hashing

**Status:** Accepted
**Date:** 2026-09-09
**Phase:** 4

## Context

RivetDB must divide the keyspace among nodes. The two established approaches
are hash partitioning (Dynamo, Cassandra, Riak) and range partitioning
(Bigtable, HBase, Spanner, CockroachDB, TiKV, YugabyteDB).

This decision is made early because it constrains almost everything else: it
determines whether ordered scans are possible, what a shard split even means,
and — critically for this project — whether load imbalance is something the
system can *fix* or merely something it avoids.

## Options

### Option A: Consistent hashing

Keys are hashed, and the hash space is divided among nodes using virtual nodes
on a ring.

**Advantages, stated fairly:**

- Load distributes uniformly almost for free. A good hash function spreads any
  key distribution evenly, so the hot-shard problem is largely designed out
  rather than managed.
- Routing needs no metadata lookup: any client that knows the ring can compute
  the owner locally. There is no range map to keep fresh, no stale-descriptor
  path, no meta-range to bootstrap from.
- Adding or removing a node moves a predictable, bounded fraction of the data.
- Simpler operationally: no split logic, no merge logic, no shard-size
  management.

**Disadvantages for this system:**

- **Ordered scans are gone.** `SCAN [a, m)` must contact every node, because
  adjacent keys hash to unrelated places. This alone rules it out for a
  transactional database that needs range reads.
- Multi-key transactions almost always span shards, since any two related keys
  land in different partitions by design. Cross-shard 2PC becomes the common
  case rather than the exception.
- **A hot *key* cannot be split.** If one key takes 40% of traffic, hashing
  offers no remedy: hashing distributes distinct keys, and a single key is
  atomic. Splitting a hash range does not divide a single key's load.
- Rebalancing is limited to moving whole partitions. There is no operation
  analogous to "split this range at a chosen point", so the control plane's
  vocabulary is much smaller.

### Option B: Range partitioning

The keyspace is divided into contiguous `[start, end)` intervals, each
replicated by its own Raft group, with an explicit descriptor recording bounds,
replicas and a generation number.

**Advantages for this system:**

- Ordered scans are local. `SCAN [a, m)` touches the one or few ranges that
  cover it — required for the client API and for any future SQL layer.
- Related keys (a common prefix, a tenant, a table) live together, so many
  multi-key transactions stay within one range and avoid 2PC entirely.
- **Ranges can be split at a chosen key.** This is the operation the entire
  rebalancing feature is built on: when a region of the keyspace is hot, the
  system can subdivide it precisely where the load is and distribute the pieces.
- Split points can be chosen from observed load, not just from size, so the
  result balances traffic rather than bytes.
- Range bounds are human-meaningful, which makes the admin API (`range list`,
  `range describe`) genuinely useful for debugging rather than a list of hash
  buckets.

**Disadvantages, stated fairly:**

- **Sequential keys create a hot range.** Monotonic keys — timestamps,
  auto-increment IDs — all land at the end of the keyspace and hammer one
  range. This is the well-known weakness, and it is real.
- Routing requires metadata: a range map that clients cache and that goes stale,
  with a stale-route retry path, a generation scheme, and a bootstrap problem
  for the metadata itself.
- Range sizes drift, requiring split and merge logic that hash partitioning does
  not need.
- More moving parts, and therefore more ways to be wrong — range bounds must
  tile the keyspace exactly, with no gap and no overlap, at all times including
  mid-split (invariants RANGE-1, RANGE-2).

## Decision

**Range partitioning**, with an explicit range descriptor carrying bounds,
replica set and a monotonically increasing generation.

The reasoning is that hash partitioning's main advantage — automatic uniform
load distribution — is the very problem RivetDB exists to solve *dynamically*.
Choosing hashing would mean designing the hot-shard problem out of the system
and, with it, the feature the project is about. Range partitioning creates the
problem and then supplies the tool to fix it: the ability to split a range at a
chosen key.

The ordered-scan requirement is independently decisive. A transactional
database whose `SCAN` must contact every node is not the system being built.

The known weakness — sequential keys concentrating on one range — is accepted
deliberately, because it is the workload the signature experiment uses. The
hot-range benchmark in Phase 9 exists precisely to demonstrate detection and
mitigation of this case. A design that could not exhibit the problem could not
demonstrate the solution.

## Consequences

- A range map must exist, be replicated, and be cached by clients. Staleness is
  a normal condition with a defined recovery path, not an error
  (architecture.md §6).
- Descriptors need generation numbers, and every request carries the generation
  the client assumed, so a replica can reject a stale request and return the
  truth (RANGE-3, RANGE-4).
- The keyspace tiling invariant must hold continuously, including during a
  split. This is checked at runtime under `invariant.Expensive()` and in the
  chaos harness (RANGE-1).
- Range IDs are never reused, so that a stale reference is always detectably
  stale rather than silently pointing at different data (RANGE-5).
- Split and merge logic must be built. Split is Phase 7; merge is not currently
  planned and is a stated limitation — without it, a workload that creates many
  ranges and then goes quiet leaves them fragmented.
- The metadata bootstrap problem must be solved: the range map lives in a range,
  which needs to be found without a range map. Open question, tracked in
  architecture.md §14.
- Key encoding matters more than under hashing. Because adjacent keys share a
  range, a schema with monotonic keys concentrates load, and the documentation
  should say so rather than let users discover it.

## Revisit if

- The metadata layer's complexity turns out to dominate the implementation,
  suggesting a hybrid (hash-prefixed ranges) would serve better.
- Splitting proves unable to mitigate realistic hot spots — for example if load
  concentrates on a single key rather than a region, which no split can divide.
  That case would need a different tool (caching, follower reads), and this ADR
  should record that it exists rather than imply splitting solves everything.
