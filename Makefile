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
check: fmt-check vet lint test race ## Run the full gate (what CI runs)

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
