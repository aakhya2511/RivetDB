# Contributing

RivetDB is correctness-first and feature-frozen. Discuss changes to durable formats,
consensus, transactions, topology protocols, or authority boundaries before coding.
Design and invariants precede implementation; failure tests precede certification.

Use Go 1.25 or later. Before submitting a change:

```bash
make check
make race
```

Documentation-only changes require make check and git diff --check. Any executable,
test, or Make semantics change requires make certify-advisor on the exact final tree.
Preserve deterministic clocks/seeds, bounded concurrency, and established contracts.

Benchmark reports must include units, sample method, exact environment, and measurement
classification. Never present constrained-disk or in-process results as production or
real-network performance.
