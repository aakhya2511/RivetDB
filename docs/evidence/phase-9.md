# Phase 9 certification evidence

## Environment

- Hardware: Apple M4, arm64
- OS: macOS 15.7.4 (24G517)
- Go: 1.25.14 darwin/arm64 (`GOTOOLCHAIN=local`)
- Repository: clean local checkout
- Build cache: `/private/tmp/rivet-phase9-closure-cache`
- Test temporary root: `/private/tmp/rivet-phase9-closure-tmp`
- Filesystem: APFS, nearly full during closure work

No disk-sensitive performance claim is made. The planner measurements are CPU
and allocation engineering baselines only.

## Closure checklist

| Mandatory item | Test/evidence | Invariant | Result |
|---|---|---|---|
| Exact 51/49, 49/51, 52/48, 48/52 sequence; sustained 80/20; protected reversal | `TestRebalanceExactAntiOscillationSequence` | REBALANCE-7, 14 | PASS; 0,0,0,0 actions, then one move, then zero reversals |
| Transient spike versus sustained load | `TestRebalanceTransientSpikeVersusSustainedLoad`, `TestHotRangeTransientSpikeDoesNotSplit` | REBALANCE-7 | PASS |
| Repeated convergence and stable no-op | `TestRebalanceRepeatedConvergenceAndLongNoop` | REBALANCE-13, 14 | PASS; score 2,000,000,000,000 → 720,000,000,000 → 80,000,000,000; 100 final no-ops |
| Balanced long run | `TestRebalanceBalancedClusterThousandNoopCycles` | REBALANCE-14 | PASS; 1,000 cycles, zero actions |
| Restart-persistent migration cooldown | `TestAutomaticRebalanceUsesCertifiedMoveReplica` | REBALANCE-7, 17 | PASS; protected reverse count 0, eligible only after clock advance |
| Sustained hot-rate split and child warmup | `TestAutomaticRebalanceSustainedHotRateSplitAndChildWarmup` | REBALANCE-4, 6, 14 | PASS; write rate 2, key `c`, SplitID 1, child samples 0, immediate actions 0 |
| Post-move actual score | `TestAutomaticRebalanceUsesCertifiedMoveReplica` | REBALANCE-18 | PASS; predicted and actual improvement 617,283,604,937 |
| Historical/latest digests and bank invariant | same automatic-migration test | REBALANCE-16 | PASS; two historical digests plus latest digest, total 1,000, mismatches 0 |
| Node failure during automatic migration | `TestAutomaticMigrationNodeFailureReconcilesOneAuthoritativeOperation` | REBALANCE-9, 15 | PASS; failure after learner add, same MigrationID, duplicates 0 |
| Node failure during automatic split | `TestAutomaticSplitNodeFailureReconcilesOneAuthoritativeOperation` | REBALANCE-9, 15 | PASS; failure after parent fence, same SplitID, duplicates 0 |
| Fresh hard-constraint validation | `TestValidateRebalancePlanRechecksMoveHardConstraints`, `TestControllerRejectsFailedTargetBeforeExecution` | REBALANCE-8, 10 | PASS; failed/full/duplicate/capacity/concurrency rejected, submissions 0 |
| Recovered-node warmup | `TestRecoveredNodeRequiresStableWarmupBeforePlacement` | REBALANCE-10 | PASS |
| Manual/automatic coexistence | `TestManualOperationsSuppressConflictingAutomaticActions` | REBALANCE-6 | PASS |
| Leader-skew convergence | `TestLeaderSkewLongRunUsesTransfersOnly` | REBALANCE-12, 13 | PASS; 10/0/0 → 3/4/3, 8 transfers, migrations 0, 100 no-ops |
| Unsatisfiable topology | `TestRebalanceUnsatisfiableTopologyRemainsNoop` | REBALANCE-3, 14 | PASS; 100 `NO_ELIGIBLE_TARGET` cycles |
| Stateful randomized normal campaign | `TestRandomizedStatefulRebalanceControllerModel` | REBALANCE-1–18 | PASS; three seeds, 30,000 events |
| Stateful randomized heavy campaign | `TestRandomizedStatefulRebalanceControllerHeavy` | REBALANCE-1–18 | PASS; seed 99001, 100,000 events |
| Abrupt controller crash matrix | `TestRebalanceControllerSubprocessCrashMatrix` | REBALANCE-9, 17 | PASS; five durable interruption points |

## Correctness defects found and fixed

Closure tests found four concrete defects rather than changing the architecture:

1. Final validation rechecked generations but not every execution-critical
   health, placement, capacity and concurrency constraint. It now does.
2. Telemetry used LSM maintenance counters, so collection could count itself
   while replicated MVCC writes were omitted. Replica request counters are now
   explicit and atomic.
3. Sampling used ReplicaID alone despite the contract identifying a sample by
   `(RangeID, ReplicaID, NodeID)`. The full tuple now keys sampler state.
4. An execution error after an authoritative operation was created could leave
   the action unlinked. The controller now discovers/persists the MigrationID
   or SplitID, remains EXECUTING and reconciles that same operation. Leader
   selection also continues past a zero-improvement eligible voter.

## Stateful randomized evidence

Normal aggregate (fixed seeds 9101 and 9102 plus replayable fresh seed
6342479344754861769; 5–10 nodes, 10–50 ranges, 30,000 events):

```text
plans=3753 moves=1251 splits=1251 leader_transfers=1251 noops=1248
cooldown_suppressions=1251 warmup_suppressions=1251 stale_rejections=1248
controller_crashes=1251 controller_restarts=1251
node_failures=1251 node_recoveries=1251
manual_moves=1248 manual_splits=1248 transactions=1248 digest_checks=1248
failed_actions=1251 completed_actions=3753 counter_resets=1248 clock_regressions=1248
reverse_moves_inside_protected_window=0 constraint_violations=0
SI_violations=0 digest_mismatches=0 duplicate_authoritative_operations=0
```

Heavy trusted controller/reference model (seed 99001; 5–10 nodes, 10–50
ranges, 100,000 events):

```text
plans=12501 moves=4167 splits=4167 leader_transfers=4167 noops=4166
cooldown_suppressions=4167 warmup_suppressions=4167 stale_rejections=4166
controller_crashes=4167 controller_restarts=4167
node_failures=4167 node_recoveries=4167
manual_moves=4166 manual_splits=4166 transactions=4166 digest_checks=4166
failed_actions=4167 completed_actions=12501 counter_resets=4166 clock_regressions=4166
reverse_moves_inside_protected_window=0 constraint_violations=0
SI_violations=0 digest_mismatches=0 duplicate_authoritative_operations=0
```

The high-event campaign models controller state and authoritative operation
submission/progress/reconciliation. Separate real-filesystem integration and
abrupt-process tests exercise the certified execution primitives.

## Planner baseline

| Ranges | Observed ns/op | B/op | allocs/op |
|---:|---:|---:|---:|
| 10 | 8,865–13,520 | 21,968 | 134 |
| 100 | 70,858–112,027 | 196,048–196,049 | 1,127 |
| 1,000 | 855,653–1,266,622 | 2,041,306–2,041,311 | 11,031 |

## Composed certification

`make certify-rebalance`: PASS on the exact closure tree. It includes the Phase
9 normal/race/stateful-100k/chaos/crash tiers and the unchanged Phase 8
migration, Phase 7 split, Phase 6 transaction, Phase 5 MVCC, Phase 4 Multi-Raft,
Phase 3 replicated-range, Phase 2 Raft and Phase 1 local-storage gates. Go
1.27.1 forward `go vet ./...`: PASS. The exact commit is recorded at closure.
