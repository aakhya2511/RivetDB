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
