# RivetDB build and verification entry points.
#
# `make check` is the gate: it runs exactly what CI runs, so a green local
# check should mean a green pipeline. Every target here is expected to work on
# a clean checkout with nothing installed but a Go toolchain, with the single
# exception of `lint`, which reports how to install golangci-lint if missing.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

GO ?= go
GOBIN ?= $(CURDIR)/bin
PKGS ?= ./...
LOCAL_PKGS ?= ./internal/clock ./internal/invariant ./internal/rlog ./internal/storage/... ./internal/testutil/...
RAFT_PKGS ?= ./internal/raft
RANGE_PKGS ?= ./internal/replicatedrange ./internal/storage/engine ./internal/storage/manifest ./internal/storage/pipeline
MULTIRAFT_PKGS ?= ./internal/multiraft
MVCC_PKGS ?= ./internal/mvcc ./internal/replicatedrange ./internal/multiraft ./internal/storage/engine ./internal/storage/manifest ./internal/storage/pipeline ./internal/storage/compaction
TXN_PKGS ?= ./internal/txn ./internal/multiraft ./internal/replicatedrange ./internal/storage/engine ./internal/storage/pipeline ./internal/storage/compaction
SPLIT_PKGS ?= ./internal/multiraft ./internal/replicatedrange ./internal/storage/engine
MIGRATION_PKGS ?= ./internal/raft ./internal/replicatedrange ./internal/multiraft

# Race tests are run twice by default. A concurrency bug that reproduces once
# in twenty runs is worth catching, and doubling a fast suite is cheap.
RACE_COUNT ?= 2

# Long-running suites (chaos campaigns, benchmarks) are excluded from the
# default gate by the -short flag and run on their own schedule.
TEST_FLAGS ?=
TEST_TIMEOUT ?= 10m

GOLANGCI_LINT_VERSION ?= v2.6.1

.PHONY: help
help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## --- Gate -------------------------------------------------------------------

.PHONY: check
check: fmt-check vet lint tidy test diff-check ## Run the practical deterministic development gate

## --- Formatting and static analysis -----------------------------------------

.PHONY: fmt
fmt: ## Rewrite sources with gofmt
	$(GO) fmt $(PKGS)

.PHONY: fmt-check
fmt-check: ## Fail if any source file is not gofmt-clean
	@unformatted=$$(gofmt -l . | grep -v '^bin/' || true); \
	if [[ -n "$$unformatted" ]]; then \
		echo "These files are not gofmt-clean; run 'make fmt':"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PKGS)

.PHONY: lint
lint: ## Run golangci-lint
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint not found. Install it with:"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)"; \
		exit 1; \
	fi
	golangci-lint run

.PHONY: tidy
tidy: ## Tidy the module graph and fail if it was not already tidy
	$(GO) mod tidy
	@git diff --exit-code -- go.mod go.sum

## --- Tests ------------------------------------------------------------------

.PHONY: test
test: ## Run the fast test suite
	$(GO) test -timeout $(TEST_TIMEOUT) $(TEST_FLAGS) $(PKGS)

.PHONY: race
race: ## Run the test suite under the race detector
	$(GO) test -race -count=$(RACE_COUNT) -timeout $(TEST_TIMEOUT) $(TEST_FLAGS) $(PKGS)

.PHONY: stress
stress: ## Run opt-in randomized and large in-process campaigns
	RIVETDB_STRESS=1 RIVETDB_PHASE1J_STRESS=1 $(GO) test -count=1 -timeout 90m $(TEST_FLAGS) $(PKGS)

.PHONY: crash
crash: ## Run subprocess and durable-publication crash campaigns
	RIVETDB_CRASH=1 $(GO) test -count=1 -timeout 30m -run 'Crash|Publication|FailureMatrix|WriteDurableBeforeAtomicApply|WALDurableBeforeMemTableApply' $(TEST_FLAGS) $(PKGS)

.PHONY: exhaustive
exhaustive: ## Run every-byte WAL, SSTable, and Manifest campaigns
	RIVETDB_EXHAUSTIVE=1 $(GO) test -count=1 -timeout 60m -run 'ExhaustiveTruncation|EveryTruncation|ManifestCrashAtEveryOffset' $(TEST_FLAGS) $(PKGS)

BENCH_FLAGS ?= -run '^$$' -bench . -benchmem -count=5

.PHONY: benchmark
benchmark: ## Run the reproducible benchmark suite (honors RIVETDB_BENCH_DIR)
	$(GO) test -timeout 120m $(BENCH_FLAGS) $(PKGS)

.PHONY: certify-local
certify-local: ## Run every local-storage correctness tier in order
	$(MAKE) check PKGS="$(LOCAL_PKGS)"
	$(MAKE) race PKGS="$(LOCAL_PKGS)"
	$(MAKE) stress PKGS="$(LOCAL_PKGS)"
	$(MAKE) crash PKGS="$(LOCAL_PKGS)"
	$(MAKE) exhaustive PKGS="$(LOCAL_PKGS)"

.PHONY: raft-test
raft-test: ## Run deterministic Raft core, persistence, partition, and simulation tests
	$(GO) test -count=1 -timeout 10m $(TEST_FLAGS) $(RAFT_PKGS)

.PHONY: raft-race
raft-race: ## Run the Raft suite twice under the race detector
	$(GO) test -race -count=$(RACE_COUNT) -timeout 15m $(TEST_FLAGS) $(RAFT_PKGS)

.PHONY: raft-stress
raft-stress: ## Run 100k-event simulation and durable Raft-store stress
	RIVETDB_RAFT_STRESS=1 $(GO) test -count=1 -timeout 30m -run 'RandomizedClusterHeavy|FileStoreDeterministicStress' $(TEST_FLAGS) $(RAFT_PKGS)

.PHONY: raft-chaos
raft-chaos: ## Run fixed and fresh 3/5-node deterministic fault schedules
	$(GO) test -count=1 -timeout 15m -run 'RandomizedClusterSafety|LeaderIsolation|Minority|DuplicateReordered|FollowerLag' $(TEST_FLAGS) $(RAFT_PKGS)

.PHONY: raft-exhaustive
raft-exhaustive: ## Run every-offset/corruption Raft-store campaigns
	$(GO) test -count=1 -timeout 15m -run 'FileStoreRejectsEveryTruncationAndCorruption|FileStoreRejectsChecksumValidSemanticCorruption|FileStorePublicationOrderingAndFailures' $(TEST_FLAGS) $(RAFT_PKGS)

.PHONY: certify-raft
certify-raft: ## Run the full Phase 2 Raft gate plus Phase 1 regression
	$(MAKE) check
	$(MAKE) raft-race
	$(MAKE) raft-stress
	$(MAKE) raft-chaos
	$(MAKE) raft-exhaustive
	$(MAKE) certify-local

.PHONY: range-test
range-test: ## Run deterministic Phase 3 integration and durable-recovery tests
	$(GO) test -count=1 -timeout 20m $(TEST_FLAGS) $(RANGE_PKGS)

.PHONY: range-race
range-race: ## Run Phase 3 integration under the race detector
	$(GO) test -race -count=1 -timeout 30m $(TEST_FLAGS) $(RANGE_PKGS)

.PHONY: range-stress
range-stress: ## Run the opt-in 100k-event replicated-range campaign
	RIVETDB_RANGE_STRESS=1 $(GO) test -count=1 -timeout 90m -run 'RandomizedReplicatedRangeHeavy' $(TEST_FLAGS) ./internal/replicatedrange

.PHONY: range-crash
range-crash: ## Run Phase 3 subprocess and Manifest-frontier crash seams
	RIVETDB_RANGE_CRASH=1 $(GO) test -count=1 -timeout 30m -run 'ReplicatedRangeSubprocessCrashRecovery|ReplicatedFrontierCrashSeams|ReplicatedPublishedSSTable' $(TEST_FLAGS) $(RANGE_PKGS)

.PHONY: certify-range
certify-range: ## Run the full Phase 3 gate plus frozen Phase 2/Phase 1 regressions
	$(MAKE) check
	$(MAKE) range-test
	$(MAKE) range-race
	$(MAKE) range-stress
	$(MAKE) range-crash
	$(MAKE) certify-raft

.PHONY: multiraft-test
multiraft-test: ## Run Phase 4 catalog, routing, hosting, and recovery tests
	$(GO) test -count=1 -timeout 30m $(TEST_FLAGS) $(MULTIRAFT_PKGS)

.PHONY: multiraft-race
multiraft-race: ## Run Phase 4 twice under the race detector
	$(GO) test -race -count=$(RACE_COUNT) -timeout 45m $(TEST_FLAGS) $(MULTIRAFT_PKGS)

.PHONY: multiraft-stress
multiraft-stress: ## Run the 25-range 100k-event Multi-Raft campaign
	RIVETDB_MULTIRAFT_STRESS=1 $(GO) test -count=1 -timeout 30m -run 'RandomizedMultiRaftHeavy' $(TEST_FLAGS) $(MULTIRAFT_PKGS)

.PHONY: multiraft-chaos
multiraft-chaos: ## Run in-memory and disk-backed Multi-Raft fault campaigns
	$(GO) test -count=1 -timeout 30m -run 'RandomizedMultiRaftSafety|RandomizedDiskBackedMultiRangeRecovery|RangeSpecificQuorum|NodeCrashHasDifferent' $(TEST_FLAGS) $(MULTIRAFT_PKGS)

.PHONY: multiraft-crash
multiraft-crash: ## Run catalog/bootstrap and multi-range subprocess crash tests
	RIVETDB_MULTIRAFT_CRASH=1 $(GO) test -count=1 -timeout 30m -run 'CatalogSubprocessCrash|BootstrapSubprocessCrash|MultiRaftSubprocessNodeCrash' $(TEST_FLAGS) $(MULTIRAFT_PKGS)

.PHONY: certify-multiraft
certify-multiraft: ## Run the Phase 4 gate and all frozen lower-phase certification
	$(MAKE) check
	$(MAKE) multiraft-test
	$(MAKE) multiraft-race
	$(MAKE) multiraft-stress
	$(MAKE) multiraft-chaos
	$(MAKE) multiraft-crash
	$(MAKE) certify-range

.PHONY: mvcc-test
mvcc-test: ## Run Phase 5 timestamp, historical-read, snapshot, and recovery tests
	$(GO) test -count=1 -timeout 30m $(TEST_FLAGS) $(MVCC_PKGS)

.PHONY: mvcc-race
mvcc-race: ## Run Phase 5 twice under the race detector
	$(GO) test -race -count=$(RACE_COUNT) -timeout 45m $(TEST_FLAGS) $(MVCC_PKGS)

.PHONY: mvcc-stress
mvcc-stress: ## Run the 100k-event Multi-Raft MVCC reference campaign
	RIVETDB_MVCC_STRESS=1 $(GO) test -count=1 -timeout 60m -run 'RandomizedMultiRaftMVCCHeavy' $(TEST_FLAGS) ./internal/multiraft

.PHONY: mvcc-chaos
mvcc-chaos: ## Run fixed/fresh historical and Multi-Raft MVCC fault campaigns
	$(GO) test -count=1 -timeout 45m -run 'RandomizedHistoricalReadsAgainstReference|RandomizedMultiRaftMVCC$$|MVCCReplicatedHistorySnapshotLeaderChangeAndRestart|MVCCNewLeaderObservesUncommitted' $(TEST_FLAGS) ./internal/storage/engine ./internal/replicatedrange ./internal/multiraft

.PHONY: mvcc-crash
mvcc-crash: ## Run abrupt timestamped-mutation crash and historical recovery
	$(GO) test -count=1 -timeout 15m -run 'MVCCSubprocessCrashHistoricalRecovery' $(TEST_FLAGS) ./internal/replicatedrange

.PHONY: certify-mvcc
certify-mvcc: ## Run the Phase 5 gate and every frozen lower-phase certification
	$(MAKE) check
	$(MAKE) mvcc-test
	$(MAKE) mvcc-race
	$(MAKE) mvcc-stress
	$(MAKE) mvcc-chaos
	$(MAKE) mvcc-crash
	$(MAKE) certify-multiraft

.PHONY: txn-test
txn-test: ## Run deterministic Snapshot Isolation, 2PC, intent, and recovery tests
	$(GO) test -count=1 -timeout 45m $(TEST_FLAGS) $(TXN_PKGS)

.PHONY: txn-race
txn-race: ## Run transaction packages under the race detector
	$(GO) test -race -count=1 -timeout 45m $(TEST_FLAGS) $(TXN_PKGS)

.PHONY: txn-stress
txn-stress: ## Run the opt-in 100k-event Snapshot Isolation model campaign
	RIVETDB_TXN_STRESS=1 $(GO) test -count=1 -timeout 45m -run '^TestRandomizedTransactionsHeavy$$' $(TEST_FLAGS) ./internal/multiraft

.PHONY: txn-chaos
txn-chaos: ## Run transaction conflicts, leader changes, range isolation, and restart recovery
	$(GO) test -count=1 -timeout 30m -run 'TransactionLeaderChanges|ConcurrentConflicts|PendingPrepared|RandomizedTransactionsAgainst' $(TEST_FLAGS) ./internal/multiraft

.PHONY: txn-crash
txn-crash: ## Run real-filesystem subprocess crashes at every durable 2PC stage
	RIVETDB_TXN_CRASH=1 $(GO) test -count=1 -timeout 30m -run '^TestTransactionSubprocessCrashMatrix$$' $(TEST_FLAGS) ./internal/multiraft

.PHONY: certify-txn
certify-txn: ## Run the Phase 6 gate and every frozen lower-phase certification
	$(MAKE) check
	$(MAKE) txn-test
	$(MAKE) txn-race
	$(MAKE) txn-stress
	$(MAKE) txn-chaos
	$(MAKE) txn-crash
	$(MAKE) certify-mvcc

.PHONY: split-test
split-test: ## Run deterministic Phase 7 metadata, transfer, MVCC, transaction, and restart tests
	$(GO) test -count=1 -timeout 60m $(TEST_FLAGS) $(SPLIT_PKGS)

.PHONY: split-race
split-race: ## Run Phase 7 packages under the race detector
	$(GO) test -race -count=1 -timeout 60m $(TEST_FLAGS) $(SPLIT_PKGS)

.PHONY: split-stress
split-stress: ## Run 100k metadata events and repeated disk-backed splits
	RIVETDB_SPLIT_STRESS=1 $(GO) test -count=1 -timeout 90m -run 'RandomizedSplitCatalogHeavy|RepeatedDiskBackedSplits' $(TEST_FLAGS) ./internal/multiraft

.PHONY: split-chaos
split-chaos: ## Run split leaders, hot traffic, transactions, lineage, and full-restart campaigns
	$(GO) test -count=1 -timeout 60m -run 'OnlineSplit|SplitKeeps|PreparedTransactionDrain|FullClusterRestart|ParentMetaAndChildLeader|StaleRouter|PreSplitBuffered|AbortBeforeFence' $(TEST_FLAGS) ./internal/multiraft

.PHONY: split-crash
split-crash: ## Run abrupt logical-image, replay, fence, and cutover subprocess crashes
	RIVETDB_SPLIT_CRASH=1 $(GO) test -count=1 -timeout 60m -run '^TestSplitSubprocessCrashMatrix$$' $(TEST_FLAGS) ./internal/multiraft

.PHONY: certify-split
certify-split: ## Run the Phase 7 gate and every frozen lower-phase certification
	$(MAKE) check
	$(MAKE) race RACE_COUNT=1
	$(MAKE) split-stress
	$(MAKE) split-chaos
	$(MAKE) split-crash
	$(MAKE) txn-stress txn-chaos txn-crash
	$(MAKE) mvcc-stress mvcc-chaos mvcc-crash
	$(MAKE) multiraft-stress multiraft-chaos multiraft-crash
	$(MAKE) range-stress range-crash
	$(MAKE) raft-stress raft-chaos raft-exhaustive
	$(MAKE) stress crash exhaustive PKGS="$(LOCAL_PKGS)"

.PHONY: migration-test
migration-test: ## Run deterministic Phase 8 membership, transfer, migration, and restart tests
	$(GO) test -count=1 -timeout 60m $(TEST_FLAGS) $(MIGRATION_PKGS)

.PHONY: migration-race
migration-race: ## Run Phase 8 packages under the race detector
	$(GO) test -race -count=1 -timeout 90m $(TEST_FLAGS) $(MIGRATION_PKGS)

.PHONY: migration-stress
migration-stress: ## Run randomized membership and repeated disk-backed migration campaigns
	RIVETDB_MIGRATION_STRESS=1 $(GO) test -count=1 -timeout 90m -run 'RandomizedMigrationMembershipHeavy|RepeatedDiskBackedMigrations' $(TEST_FLAGS) ./internal/multiraft

.PHONY: migration-chaos
migration-chaos: ## Run learner, online traffic, leadership, identity, and reconciliation campaigns
	$(GO) test -count=1 -timeout 90m -run 'Migration|Migrate|RetiredSource|Learner|Joint|ConfigurationSurvivesSnapshot' $(TEST_FLAGS) ./internal/raft ./internal/replicatedrange ./internal/multiraft

.PHONY: migration-crash
migration-crash: ## Run abrupt Phase 8 snapshot, configuration, cutover, and deletion crashes
	RIVETDB_MIGRATION_CRASH=1 $(GO) test -count=1 -timeout 60m -run '^TestMigrationSubprocessCrashMatrix$$' $(TEST_FLAGS) ./internal/multiraft

.PHONY: certify-migration
certify-migration: ## Run the Phase 8 gate and every frozen lower-phase certification
	$(MAKE) check
	$(MAKE) race RACE_COUNT=1
	$(MAKE) migration-stress
	$(MAKE) migration-chaos
	$(MAKE) migration-crash
	$(MAKE) split-stress split-chaos split-crash
	$(MAKE) txn-stress txn-chaos txn-crash
	$(MAKE) mvcc-stress mvcc-chaos mvcc-crash
	$(MAKE) multiraft-stress multiraft-chaos multiraft-crash
	$(MAKE) range-stress range-crash
	$(MAKE) raft-stress raft-chaos raft-exhaustive
	$(MAKE) stress crash exhaustive PKGS="$(LOCAL_PKGS)"

.PHONY: diff-check
diff-check: ## Fail on whitespace errors in the working diff
	git diff --check

.PHONY: cover
cover: ## Produce a coverage profile and an HTML report
	@mkdir -p $(GOBIN)
	$(GO) test -covermode=atomic -coverprofile=$(GOBIN)/coverage.out -timeout $(TEST_TIMEOUT) $(PKGS)
	$(GO) tool cover -func=$(GOBIN)/coverage.out | tail -1
	$(GO) tool cover -html=$(GOBIN)/coverage.out -o $(GOBIN)/coverage.html
	@echo "report: $(GOBIN)/coverage.html"

# Replay a specific randomized-test seed:
#   make seed SEED=8134472901 RUN=TestSomething
.PHONY: seed
seed: ## Replay a randomized test with a fixed seed (SEED=... RUN=...)
	@if [[ -z "$${SEED:-}" ]]; then echo "usage: make seed SEED=<int64> [RUN=<pattern>]"; exit 1; fi
	RIVETDB_SEED=$(SEED) $(GO) test -count=1 -timeout $(TEST_TIMEOUT) \
		$(if $(RUN),-run '$(RUN)',) $(PKGS)

.PHONY: promote-seed
promote-seed: ## Promote a reproduced failure (PACKAGE=... TEST=... SEED=...)
	@if [[ -z "$${PACKAGE:-}" || -z "$${TEST:-}" || -z "$${SEED:-}" ]]; then \
		echo "usage: make promote-seed PACKAGE=./internal/pkg TEST=TestName SEED=<int64>"; \
		exit 1; \
	fi
	$(GO) run ./internal/testutil/cmd/promote-seed \
		-package "$(PACKAGE)" -test "$(TEST)" -seed "$(SEED)"

## --- Build ------------------------------------------------------------------

.PHONY: build
build: ## Compile all packages
	$(GO) build $(PKGS)

.PHONY: clean
clean: ## Remove build and coverage artifacts
	rm -rf $(GOBIN)
	$(GO) clean -testcache
