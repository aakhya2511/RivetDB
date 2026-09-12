# Architecture diagrams

These diagrams show implemented in-process boundaries. Dashed arrows are advisory.

## System overview

```mermaid
flowchart LR
  C[Caller] --> R[Router] --> MR[Multi-Raft range] --> M[MVCC state machine] --> L[LSM engine]
  Meta[Replicated MetaRange] --> R
  Meta --> MR
```

## Write path

```mermaid
sequenceDiagram
  participant C as Caller
  participant R as Router
  participant L as Range leader
  participant F as Raft followers
  participant S as MVCC/LSM state machine
  C->>R: mutation(key, generation)
  R->>L: route
  L->>F: replicate Raft entry
  F-->>L: quorum acknowledgements
  L->>S: deterministic committed apply
  L-->>C: result
```

## Distributed transaction

```mermaid
flowchart LR
  C[Coordinator host] --> P1[Prepare participant A]
  C --> P2[Prepare participant B]
  P1 --> C
  P2 --> C
  C --> T{Replicated COMMITTED or ABORTED record}
  T --> R[Resolve intents]
```

The replicated transaction record, not coordinator memory, is recovery authority.

## Online range split

```mermaid
flowchart LR
  P[Parent logical image] --> L[Shadow left child]
  P --> R[Shadow right child]
  L --> D[Replay committed deltas]
  R --> D
  D --> F[Short parent fence] --> C[MetaRange generation CAS]
  C --> A[Children active; parent redirects]
```

## Replica migration

```mermaid
flowchart LR
  S[Source voter] --> B[Bootstrap learner] --> C[Raft catch-up]
  C --> J[Joint configuration] --> F[Final configuration] --> R[Retire source]
```

## Safe AI boundary

```mermaid
flowchart LR
  T[Bounded telemetry] --> A[AI recommendation]
  A -.-> H[Human approval] -.-> P[Deterministic planner]
  P --> V[Fresh-state validator] --> C[Candidate for certified host]
```

There is no AI-to-Raft, AI-to-MetaRange, or advisor execution edge.
