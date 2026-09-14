# Local tasks aligned with .github/workflows/ci.yml (plus common extras).

GOLANGCI_LINT_VERSION ?= v2.11.4
TOOL_DIR := $(abspath bin)
GOLANGCI_LINT := $(TOOL_DIR)/golangci-lint

.PHONY: help ci check mod-download mod-verify tools tool-golangci-lint lint vet build test tidy clean clean-tools run

help:
	@echo "Targets:"
	@echo "  make ci                 - mod download, verify, lint, build, test (same as CI)"
	@echo "  make mod-download       - go mod download"
	@echo "  make mod-verify         - go mod verify"
	@echo "  make tools              - install dev tools (golangci-lint -> bin/)"
	@echo "  make tool-golangci-lint - install golangci-lint $(GOLANGCI_LINT_VERSION) into bin/"
	@echo "  make lint               - run golangci-lint (installs tool into bin/ if missing)"
	@echo "  make vet                - go vet ./..."
	@echo "  make build              - go build -v ./..."
	@echo "  make test               - go test -v -count=1 ./..."
	@echo "  make tidy               - go mod tidy"
	@echo "  make check              - vet + ci"
	@echo "  make run                - run sensor with example config (override CONFIG=... LISTEN=...)"
	@echo "  make clean              - remove coverage artifacts"
	@echo "  make clean-tools        - remove bin/golangci-lint"

.DEFAULT_GOAL := ci

CONFIG ?= config/sensor.yaml
LISTEN ?= localhost:24200

ci: mod-download mod-verify lint build test

check: vet ci

mod-download:
	go mod download

mod-verify:
	go mod verify

tools: tool-golangci-lint

tool-golangci-lint: $(GOLANGCI_LINT)

$(GOLANGCI_LINT):
	mkdir -p $(TOOL_DIR)
	GOBIN=$(TOOL_DIR) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --timeout=5m ./...

vet:
	go vet ./...

build:
	go build -v ./...

test:
	go test -v -count=1 ./...

tidy:
	go mod tidy

clean:
	rm -f coverage.out coverage.html

clean-tools:
	rm -f $(GOLANGCI_LINT)

run:
	go run ./cmd/modelsrv-prometheus-sensor --config $(CONFIG) --listen $(LISTEN)
