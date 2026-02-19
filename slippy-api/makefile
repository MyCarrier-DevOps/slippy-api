SHELL:=/bin/bash

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
BINARY := slippy-api

.PHONY: lint
lint: install-tools
	@echo "Linting module..."
	go mod tidy
	golangci-lint run --config .github/.golangci.yml --timeout 5m ./...

.PHONY: test
test:
	@echo "Running tests..."
	go mod download
	go test -race -cover -coverprofile=coverage.out ./...
	go tool cover -func coverage.out

.PHONY: clean
clean:
	@echo "Cleaning..."
	go clean ./...
	go clean -testcache
	rm -f $(BINARY) coverage.out

.PHONY: fmt
fmt: install-tools
	@echo "Formatting..."
	golangci-lint fmt --config .github/.golangci.yml ./...

.PHONY: bump
bump:
	@echo "Bumping module versions..."
	go get -u
	go mod tidy

.PHONY: tidy
tidy:
	@echo "Tidying up module dependencies..."
	go mod tidy

.PHONY: check-sec
check-sec:
	@echo "Checking security vulnerabilities..."
	go mod download
	go install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck -show verbose ./...

.PHONY: build
build:
	@echo "Building $(BINARY)..."
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o $(BINARY) .

.PHONY: install-tools
install-tools:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $$(go env GOPATH)/bin v2.5.0
