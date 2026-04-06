# Contributing to slippy-api

Thank you for contributing to the Slippy API. This guide covers everything you need to get started, from local setup to submitting a pull request.

## Prerequisites

- **Go 1.26+**
- **Container runtime** (Docker or Podman) -- required for e2e tests
- **golangci-lint** -- installed automatically by `make install-tools`
- **oapi-codegen** -- installed automatically by `make install-oapi-codegen` (only needed when changing API contracts)

## Repository Structure

This repo contains two Go modules:

| Module | Type | Purpose |
|---|---|---|
| `slippy-api/` | Application | The API server (has a `main` package, produces a binary) |
| `slippy-client/` | Library | Generated Go client for the v1 API (no `main` package) |

```
slippy-api/             # Repository root
├── makefile            # Build, test, lint, format, code generation
├── CONTRIBUTING.md     # This file
├── VERSIONING.md       # API versioning guide
├── docs/               # Additional documentation
├── bin/                # Build output (gitignored)
├── slippy-api/         # API server module
│   ├── Dockerfile
│   ├── go.mod
│   ├── main.go
│   ├── api/v1/         # Generated OpenAPI specs
│   └── internal/       # Application code (domain, handler, infrastructure, middleware, config, telemetry)
└── slippy-client/      # Generated Go client module
    ├── go.mod
    ├── oapi-codegen.yaml
    └── client.gen.go   # DO NOT EDIT — generated from the OpenAPI spec
```

## Getting Started

### 1. Clone and build

```bash
git clone git@github.com:MyCarrier-DevOps/slippy-api.git
cd slippy-api
make build
```

The binary is output to `./bin/slippy-api`.

### 2. Run tests

```bash
make test
```

This runs all unit, integration, and e2e tests across both modules with coverage reporting. E2e tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up a real Redis container.

To skip e2e tests (no container runtime required):

```bash
cd slippy-api && go test -short ./...
```

### 3. Lint and format

```bash
make fmt
make lint
```

Both must pass with zero issues before submitting a PR.

## Development Workflow

### Branch and commit

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feat/my-feature main
   ```
2. Make your changes, following the code quality standards below.
3. Run the full validation suite before pushing:
   ```bash
   make fmt && make lint && make test
   ```
4. Push your branch and open a pull request against `main`.

### Code quality gates

Every PR must pass these checks (enforced by CI):

| Check | Command | Requirement |
|---|---|---|
| **Unit tests** | `make test` | All pass |
| **Coverage** | Reported by `make test` | >= 80% on `slippy-api` |
| **Lint** | `make lint` | Zero issues |
| **Vulnerability scan** | `make check-sec` | No known vulnerabilities |

### Makefile targets

| Target | What it does |
|---|---|
| `make build` | Build the `slippy-api` binary into `./bin/` |
| `make test` | Run all tests with coverage for both modules |
| `make lint` | Run golangci-lint on both modules |
| `make fmt` | Format code with golangci-lint (goimports + golines) |
| `make clean` | Remove build artifacts and test cache |
| `make tidy` | Run `go mod tidy` on both modules |
| `make bump` | Update all dependencies to latest |
| `make check-sec` | Run `govulncheck` security audit |
| `make generate-spec` | Regenerate OpenAPI specs from the running API |
| `make generate-client` | Regenerate specs + Go client (runs `generate-spec` first) |
| `make install-tools` | Install golangci-lint |
| `make install-oapi-codegen` | Install oapi-codegen |

## Best Practices

### Code style

- **Formatting**: `make fmt` uses golangci-lint with `goimports` and `golines` (120 char max line length). Always run before committing.
- **Linting**: The linter config at `.github/.golangci.yml` enables 30+ linters. Run `make lint` early and often -- don't accumulate lint debt.
- **Naming**: Follow standard Go conventions. Interfaces end in `-er` (`SlipReader`, `ImageTagReader`). Avoid stuttering (`handler.SlipHandler` not `handler.Handler`).

### Testing

- **Write tests first** when possible. The codebase follows test-driven development practices.
- **Unit tests** go next to the code they test (`foo.go` -> `foo_test.go`).
- **Table-driven tests** are preferred for testing multiple scenarios.
- **Use `testify`** (`assert` and `require`) for assertions. Use `require` for preconditions that must pass, `assert` for the actual checks.
- **Mock at boundaries**: Use interfaces (`SlipReader`, `ImageTagReader`, `CIJobLogReader`) for dependency injection. Create stub implementations in test files.
- **E2e tests** live in `internal/e2e/` and use testcontainers. They require a container runtime. Use `testing.Short()` to skip them in fast-feedback loops.
- **Coverage target**: 80% minimum on `slippy-api`. Generated code (`slippy-client`) is exempt.

### Architecture

- **Clean Architecture**: Domain interfaces in `internal/domain/`, implementations in `internal/infrastructure/`, HTTP layer in `internal/handler/`.
- **Dependency injection**: All dependencies are wired in `main.go` via `buildHandler()`. Handlers and infrastructure receive interfaces, not concrete types.
- **Decorator pattern**: New cross-cutting concerns (caching, tracing, retries) should wrap existing interfaces as decorators, not modify the underlying implementation.
- **Handler layer owns HTTP concerns**: Request/response types, route registration, and error mapping live in `internal/handler/`. Domain types should not import `net/http`.

### Error handling

- **Wrap errors with context**: `fmt.Errorf("clickhouse store: %w", err)`.
- **Map errors at boundaries**: The `mapError()` functions in handlers translate domain errors to HTTP status codes.
- **Don't swallow errors**: Log and return, or return -- never silently drop.

### API changes

When you modify the API surface (endpoints, request/response types, parameters):

1. Make your changes in the handler/domain layer.
2. Run `make generate-client` to regenerate the OpenAPI spec and Go client.
3. Review the diff in `slippy-api/api/v1/` and `slippy-client/client.gen.go`.
4. Commit the regenerated files alongside your code changes.

See [docs/contract-regeneration.md](docs/contract-regeneration.md) for the full step-by-step guide.

### API versioning

All endpoints are served at both unversioned (legacy) and `/v1/` prefixed paths. New integrations should use `/v1/` paths.

See [docs/versioning-api.md](docs/versioning-api.md) for the versioning strategy, breaking vs non-breaking change definitions, and how to introduce future versions.

## Building the Docker Image

The Dockerfile uses a multi-stage build:

```bash
cd slippy-api
docker build -t slippy-api .
```

This produces a minimal Alpine-based image (~50MB) with the statically-linked binary. The build stage uses `CGO_ENABLED=0` for a fully static binary.

To run locally:

```bash
docker run -p 8080:8080 \
  -e SLIPPY_API_KEY=my-key \
  -e SLIPPY_PIPELINE_CONFIG='{"version":"1.0","name":"test","steps":[{"name":"build","description":"build"}]}' \
  -e SLIPPY_GITHUB_APP_ID=12345 \
  -e SLIPPY_GITHUB_APP_PRIVATE_KEY=/path/to/key.pem \
  -e CLICKHOUSE_HOSTNAME=clickhouse.example.com \
  -e CLICKHOUSE_USERNAME=slippy \
  -e CLICKHOUSE_PASSWORD=secret \
  -e CLICKHOUSE_DATABASE=ci \
  slippy-api
```

The API will be available at `http://localhost:8080`. Verify with:

```bash
curl http://localhost:8080/health
# {"status":"ok"}

curl http://localhost:8080/v1/health
# {"status":"ok"}
```

## Building as a Standalone Artifact

For deployment without Docker (e.g., direct binary deployment):

```bash
# Build for the current platform
make build
# Output: ./bin/slippy-api

# Cross-compile for Linux (typical for server deployment)
GOOS=linux GOARCH=amd64 make build
# Output: ./bin/slippy-api (Linux amd64 binary)

# Cross-compile for ARM (e.g., AWS Graviton)
GOOS=linux GOARCH=arm64 make build
```

The binary is statically linked (`CGO_ENABLED=0`) and has no runtime dependencies beyond the OS. Copy it to your target server and run with the required environment variables set.

## CI/CD

The GitHub Actions workflow at `.github/workflows/ci.yaml` runs on every push and PR to `main`:

- **Unit tests** with 80% coverage threshold (`slippy-api` module)
- **Lint** on both `slippy-api` and `slippy-client` modules
- **Vulnerability scan** via `govulncheck`
- **Tag and release** `slippy-client` when its files change on main (uses GitVersion for semver)

## Getting Help

- Open an issue at [github.com/MyCarrier-DevOps/slippy-api/issues](https://github.com/MyCarrier-DevOps/slippy-api/issues)
- Check the API documentation at `/docs` when the server is running
- See the generated OpenAPI spec at `slippy-api/api/v1/openapi.json`
