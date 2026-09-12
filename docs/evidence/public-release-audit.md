# Public release audit

## Baseline and scope

- Baseline: clean main at 18f061bd79661a7495547c8a8c65666762cfe181.
- Audited: all 263 tracked files and all commits reachable from refs.
- Changes: documentation, diagrams, public guidance, and removal of two personal paths.
- Final commit is recorded in the release report; a commit cannot contain its own hash.

## Public-risk and secret audit

The tracked tree was searched for development provenance, conversation/interview
language, temporary debugging text, absolute personal paths, credentials, tokens,
private keys, passwords, authorization headers, and database secrets. History scanning
used commit-reachable content and printed filenames only. No secret or development
provenance was found. Legitimate optional-advisor terminology remains.

Two evidence files exposed a personal checkout path; both now say “clean local
checkout.” Earlier commits retain that non-secret path because release preparation does
not rewrite history. Generic /private/tmp references remain where they document
reproducible benchmark/test environments, not personal data.

## Claim, artifact, and history audit

README claims were checked against design notes, stable invariants, executable tests,
and measured evidence. Snapshot Isolation, in-process distribution, constrained disk
measurements, and missing functionality are explicit. No production-ready, formally
verified, Jepsen-certified, linearizable-read, serializable, cloud-scale, or zero-downtime
claim is made.

| Claim | Classification | Evidence / qualification |
|---|---|---|
| Custom LSM engine | SUPPORTED | storage design, formats, engine and every-offset recovery tests |
| Raft and Multi-Raft | SUPPORTED | independent range groups, durable stores, simulator and crash campaigns |
| MVCC historical reads | SUPPORTED | timestamped storage plus historical reference/digest tests |
| Snapshot Isolation and 2PC | SUPPORTED | transaction protocol, conflict model and crash matrix; not serializable |
| Online range split | SUPPORTED | live-write, fence, image/delta, cutover and stale-route tests |
| Replica migration and joint consensus | SUPPORTED | learner bootstrap, catch-up, joint/final membership and retirement tests |
| Automatic rebalancing | SUPPORTED | deterministic telemetry/planner/controller and recovery tests |
| Compositional chaos | SUPPORTED | one-million-event model and five-node filesystem recovery campaign |
| Optional AI advisor | QUALIFIED | advisory only, offline-certified, human/validator gated, no live adapter |
| Distributed performance | QUALIFIED | simulator/in-process only; no real-network or multi-host claim |
| Disk performance | QUALIFIED | nearly-full APFS constrained baselines, not representative results |

No tracked profile, log, archive, cache, coverage file, or binary was found. The largest
tracked file is under 60 KiB. The gitignore covers build/test outputs, profiles,
coverage, benchmark scratch data, local data, editors, and OS files. Apache-2.0 is
present. Reachable commit messages contain no material public-release concern; history
was not rewritten.

## Certification

Because only documentation and repository ignore metadata changed, the release gate is
make check, git diff --check, link/diagram validation, and final public-risk scans.
Results, clean status, and the final commit are recorded in the release report.
