SHELL:=/bin/bash

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
MODULES  := slippy-api slippy-client slippy-migrator
BINARIES := slippy-api slippy-migrator

# NOTE: the `|| exit 1` on every per-module loop below is load-bearing. Without it a
# loop's exit status is whatever the LAST module returned, so a slippy-api failure was
# masked by a passing slippy-migrator and the recipe exited 0. CLAUDE.md names
# `make lint && make test` the final gate before commit, so a masked failure defeats the
# gate; `make build` was worse still, exiting 0 while leaving a stale binary in bin/ that
# looked freshly built. Every loop that can fail now carries it. CI is unaffected either
# way (per-module matrix in .github/workflows/ci.yaml).

.PHONY: lint
lint: install-tools
	@echo "Linting all modules..."
	@for dir in $(MODULES); do \
		if [ -d "$$dir" ]; then \
			echo "Linting $$dir module..."; \
			(cd $$dir && go mod tidy && golangci-lint run --config ../.github/.golangci.yml --timeout 5m ./...) || exit 1; \
		else \
			echo "Directory $$dir not found, skipping..."; \
		fi; \
	done

.PHONY: test
test:
	@echo "Testing all modules..."
	@for dir in $(MODULES); do \
		if [ -d "$$dir" ]; then \
			if [ "$$dir" != "slippy-client" ]; then \
				echo "Testing $$dir module..."; \
				(cd $$dir && go mod download && go test -cover -coverprofile=coverage.out ./... && go tool cover -func coverage.out ) || exit 1; \
			fi; \
		else \
			echo "Directory $$dir not found, skipping..."; \
		fi; \
	done

.PHONY: clean
clean:
	@echo "Cleaning all modules..."
	@for dir in $(MODULES); do \
		if [ -d "$$dir" ]; then \
			echo "Cleaning $$dir module..."; \
			(cd $$dir && go clean ./... && go clean -testcache) || exit 1; \
		else \
			echo "Directory $$dir not found, skipping..."; \
		fi; \
	done

.PHONY: fmt
fmt: install-tools
	@echo "Formatting all modules..."
	@for dir in $(MODULES); do \
		if [ -d "$$dir" ]; then \
			echo "Formatting $$dir module..."; \
			(cd $$dir && golangci-lint fmt --config ../.github/.golangci.yml ./...) || exit 1; \
		else \
			echo "Directory $$dir not found, skipping..."; \
		fi; \
	done

.PHONY: bump
bump:
	@echo "Bumping module versions..."
	@for dir in $(MODULES); do \
		if [ -d "$$dir" ]; then \
			echo "Bumping $$dir module..."; \
			(cd $$dir && go get -u && go mod tidy ) || exit 1; \
		else \
			echo "Directory $$dir not found, skipping..."; \
		fi; \
	done

.PHONY: tidy
tidy:
	@echo "Tidying up module dependencies..."
	@for dir in $(MODULES); do \
		if [ -d "$$dir" ]; then \
			echo "Tidying $$dir module..."; \
			(cd $$dir && go mod tidy ) || exit 1; \
		else \
			echo "Directory $$dir not found, skipping..."; \
		fi; \
	done

.PHONY: check-sec
# govulncheck pinned to v1.3.0: v1.4.0 panics ("ForEachElement called on type
# containing *types.TypeParam") under Go 1.26 generics (huma). Unpin when a
# govulncheck >v1.4.0 fixes the regression.
check-sec:
	@echo "Checking security vulnerabilities..."
	@for dir in $(or $(PKG),$(MODULES)); do \
		if [ -d "$$dir" ]; then \
			echo "Checking $$dir module..."; \
			(cd $$dir && go mod download && go install golang.org/x/vuln/cmd/govulncheck@v1.3.0 && govulncheck -show verbose ./...) || exit 1; \
		else \
			echo "Directory $$dir not found, skipping..."; \
		fi; \
	done

.PHONY: build
build:
	@mkdir -p bin
	@echo "Building $(BINARIES)..."
	@for dir in $(BINARIES); do \
		if [ -d "$$dir" ]; then \
			echo "Building $$dir -> bin/$$(basename $$dir)"; \
			(cd $$dir && CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o ../bin/$$(basename $$dir) .) || exit 1; \
		else \
			echo "Directory $$dir not found, skipping..."; \
		fi; \
	done

.PHONY: generate-spec
generate-spec:
	@echo "Generating OpenAPI spec..."
	(cd slippy-api && GENERATE_SPEC=1 go test -run TestGenerateOpenAPISpec -count=1 ./...)

.PHONY: generate-client
generate-client: generate-spec install-oapi-codegen
	@echo "Generating Go client from OpenAPI spec..."
	(cd slippy-client && oapi-codegen -config oapi-codegen.yaml ../slippy-api/api/v1/openapi-v1.json)
	(cd slippy-client && go mod tidy)

.PHONY: install-oapi-codegen
install-oapi-codegen:
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.6.0

GOLANGCI_VERSION := v2.11.3
GOLANGCI_INSTALLER_SHA := 6008b81b81c690c046ffc3fd5bce896da715d5fd

# The installer is fetched by IMMUTABLE COMMIT SHA, and the pipeline may not mask a fetch
# failure.
#
# Why a SHA and not the tag: a tag is a movable ref. golangci-lint does enforce tag
# protection on ~ALL tags today (repo ruleset 1382708), so retagging v2.11.3 is blocked
# upstream — but that is a policy this consumer cannot verify at fetch time and an org
# owner can change. A SHA needs no trust in upstream policy at all. 6008b81 is what
# v2.11.3 points at, and install.sh is byte-identical at both refs (verified:
# sha256 edfa587f31bde70db161d1e5b783e086a1627d7e2f7c91de5f7cca79bcdf8631). The tag stays
# as the version ARGUMENT, which is what install.sh expects.
#
# Scope: this pins the SCRIPT only. install.sh derives TARBALL_URL and CHECKSUM_URL from
# the release tag and verifies one against the other, so the binary itself stays
# tag-resolved; upstream tag protection covers that half. Pinning the script closes the
# more important half, because the script executes before any verification it performs.
#
# Why pipefail: a pipeline exits with its LAST command's status, so a failed curl was
# swallowed — sh read empty stdin and exited 0, leaving whatever golangci-lint was already
# on PATH. That is the same masked-failure class the loops above fix, and pinning sharpens
# it: HEAD always resolved, a SHA can 404. SHELL is /bin/bash (line 1), so pipefail is
# available. Note this converts silent-nothing into silent-partial — curl | sh still
# executes the prefix of a truncated transfer. Downloading to a temp file and verifying a
# recorded sha256(install.sh) before executing is the only construction that fully closes
# it; that is a follow-up.
#
# Deliberately NOT `go install`: golangci-lint's own install docs state that go install
# "aren't guaranteed to work" and recommend the binary installation used here.
.PHONY: install-tools
install-tools:
	set -o pipefail; curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/$(GOLANGCI_INSTALLER_SHA)/install.sh | sh -s -- -b `go env GOPATH`/bin $(GOLANGCI_VERSION)
