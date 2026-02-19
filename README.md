# slippy-api

Read-only REST API for querying CI/CD routing slips stored in ClickHouse. Built with [huma v2](https://huma.rocks/) on Go's standard library `net/http`, with optional Dragonfly/Redis caching and OpenTelemetry instrumentation.

## Overview

Slippy API provides a lightweight, read-only HTTP interface for querying routing slips — the state-tracking records that follow a code change through the CI/CD pipeline. Each routing slip captures a correlation ID, repository, branch, commit SHA, pipeline step statuses, and a full audit history.

The API is backed by the shared [`goLibMyCarrier/slippy`](https://github.com/MyCarrier-DevOps/goLibMyCarrier/tree/main/slippy) library for ClickHouse persistence and exposes an auto-generated OpenAPI 3.1 specification.

## Architecture

```
┌────────────-─┐     ┌──────────────┐     ┌───────────────────┐     ┌────────────┐
│   Client     │────▶│  Auth        │────▶│  Handler          │────▶│ ClickHouse │
│  (Bearer)    │     │  Middleware  │     │  (huma routes)    │     │  (slippy)  │
└────────────-─┘     └──────────────┘     └───────────────────┘     └────────────┘
                                               │
                                               ▼ (optional)
                                         ┌───────────┐
                                         │ Dragonfly │
                                         │  Cache    │
                                         └───────────┘
```

The application follows Clean Architecture with clear dependency boundaries:

| Layer | Package | Responsibility |
|---|---|---|
| **Domain** | `internal/domain` | `SlipReader` interface, type aliases for upstream `Slip`/`SlipWithCommit` |
| **Infrastructure** | `internal/infrastructure` | `SlipStoreAdapter` (read-only adapter over `slippy.SlipStore`), `CachedSlipReader` (Dragonfly/Redis decorator) |
| **Handler** | `internal/handler` | HTTP route registration, request/response types, error mapping |
| **Middleware** | `internal/middleware` | Bearer token authentication with constant-time comparison |
| **Config** | `internal/config` | Environment variable loading and validation |
| **Main** | `main.go` | Wiring, server startup, graceful shutdown |

## API Endpoints

All `/slips/*` endpoints require a `Bearer` token in the `Authorization` header.

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check (no auth required). Returns `{"status":"ok"}` |
| `GET` | `/slips/{correlationID}` | Get a routing slip by its correlation ID |
| `GET` | `/slips/by-commit/{owner}/{repo}/{commitSHA}` | Get a routing slip by repository and commit SHA |
| `POST` | `/slips/find-by-commits` | Find the first matching slip for an ordered list of commits |
| `POST` | `/slips/find-all-by-commits` | Find all matching slips for a list of commits |
| `GET` | `/openapi.json` | Auto-generated OpenAPI 3.1 specification |
| `GET` | `/docs` | Interactive API documentation (Stoplight Elements) |

### Request/Response Examples

**GET /slips/{correlationID}**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  https://slippy-api.example.com/slips/abc-123-def
```

```json
{
  "correlation_id": "abc-123-def",
  "repository": "MyCarrier-DevOps/my-service",
  "branch": "main",
  "commit_sha": "a1b2c3d4e5f6...",
  "status": "in_progress",
  "created_at": "2026-02-19T08:00:00Z",
  "updated_at": "2026-02-19T08:05:00Z",
  "steps": { ... },
  "aggregates": { ... }
}
```

**POST /slips/find-by-commits**

```bash
curl -X POST -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"repository":"MyCarrier-DevOps/my-service","commits":["a1b2c3","d4e5f6"]}' \
  https://slippy-api.example.com/slips/find-by-commits
```

```json
{
  "slip": { "correlation_id": "abc-123-def", "..." : "..." },
  "matched_commit": "a1b2c3"
}
```

### Error Responses

| Status | Condition |
|---|---|
| `400` | Invalid correlation ID or invalid repository format |
| `401` | Missing or malformed `Authorization` header |
| `403` | Invalid API key |
| `404` | Slip not found |
| `500` | Internal server error |

All errors follow the standard format:

```json
{
  "status": 403,
  "title": "invalid API key"
}
```

## Configuration

All configuration is via environment variables. No config files, no Vault.

### Required

| Variable | Description | Example |
|---|---|---|
| `SLIPPY_API_KEY` | Bearer token for API authentication | `my-secret-key` |
| `CLICKHOUSE_HOSTNAME` | ClickHouse server hostname | `clickhouse.example.com` |
| `CLICKHOUSE_USERNAME` | ClickHouse username | `slippy` |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | `***` |
| `CLICKHOUSE_DATABASE` | ClickHouse database name | `ci` |

### Optional

| Variable | Description | Default |
|---|---|---|
| `PORT` | HTTP server listen port | `8080` |
| `CLICKHOUSE_PORT` | ClickHouse port | `9440` |
| `CLICKHOUSE_SKIP_VERIFY` | Skip TLS verification | `false` |
| `DRAGONFLY_HOST` | Dragonfly/Redis host (enables caching when set) | _(disabled)_ |
| `DRAGONFLY_PORT` | Dragonfly/Redis port | `6379` |
| `DRAGONFLY_PASSWORD` | Dragonfly/Redis password | _(empty)_ |
| `CACHE_TTL` | Cache entry time-to-live (Go duration) | `10m` |

> Caching is automatically enabled when `DRAGONFLY_HOST` is set. If the Dragonfly ping fails at startup, caching is disabled gracefully and the API falls through to ClickHouse directly.

### OpenTelemetry

The API initialises the OpenTelemetry SDK at startup when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Both traces and metrics are exported.

| Variable | Description | Default |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector endpoint (enables SDK when set) | _(disabled)_ |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | Export protocol: `grpc` or `http/protobuf` | `grpc` |
| `OTEL_SERVICE_NAME` | Service name in traces/metrics | `slippy-api` |
| `OTEL_SDK_DISABLED` | Disable SDK entirely | `false` |
| `OTEL_RESOURCE_ATTRIBUTES_NODE_NAME` | Kubernetes node name | _(empty)_ |
| `OTEL_RESOURCE_ATTRIBUTES_POD_NAME` | Kubernetes pod name | _(empty)_ |
| `OTEL_RESOURCE_ATTRIBUTES_POD_NAMESPACE` | Kubernetes pod namespace | _(empty)_ |
| `OTEL_RESOURCE_ATTRIBUTES_POD_UID` | Kubernetes pod UID | _(empty)_ |

The `OTEL_RESOURCE_ATTRIBUTES_*` variables are typically injected via the Kubernetes downward API and appear as resource attributes on all exported telemetry. When deploying with the `mycarrier-helm` chart, these are set automatically.

## Project Structure

```
slippy-api/                          # Repository root
├── .github/
│   └── .golangci.yml                # golangci-lint v2 configuration
├── makefile                         # Build, test, lint, fmt targets
├── README.md
└── slippy-api/                      # Go module
    ├── Dockerfile
    ├── go.mod
    ├── main.go                      # Entrypoint, wiring, graceful shutdown
    ├── main_test.go                 # buildHandler + connectCache + run() tests
    └── internal/
        ├── config/
        │   ├── config.go            # Environment variable loading
        │   └── config_test.go
        ├── domain/
        │   └── slip.go              # SlipReader interface, type aliases
        ├── handler/
        │   ├── health.go            # GET /health
        │   ├── health_test.go
        │   ├── slip_handler.go      # Slip CRUD routes + error mapping
        │   └── slip_handler_test.go
        ├── infrastructure/
        │   ├── cache.go             # CachedSlipReader (Dragonfly decorator)
        │   ├── cache_test.go
        │   ├── store.go             # SlipStoreAdapter (read-only adapter)
        │   └── store_test.go
        ├── middleware/
        │   ├── auth.go              # Bearer token auth middleware
        │   └── auth_test.go
        ├── telemetry/
        │   ├── telemetry.go         # OTel SDK init (traces + metrics)
        │   └── telemetry_test.go
        └── e2e/
            └── e2e_test.go          # Full-stack e2e with testcontainers Redis
```

## Development

### Prerequisites

- Go 1.26+
- Container runtime (Podman or Docker) — for e2e tests
- ClickHouse — for integration/production
- Dragonfly/Redis — optional, for caching

### Build

```bash
make build
```

### Test

```bash
make test
```

This runs all unit, integration, and e2e tests with coverage reporting. The e2e tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up a real Redis container.

To skip e2e tests (no container runtime required):

```bash
cd slippy-api && go test -short ./...
```

### Lint

```bash
make lint
```

Uses [golangci-lint v2](https://golangci-lint.run/) with the configuration at `.github/.golangci.yml`.

### Format

```bash
make fmt
```

### Other Targets

| Target | Description |
|---|---|
| `make clean` | Clean build artifacts and test cache |
| `make tidy` | Run `go mod tidy` |
| `make bump` | Update all dependencies to latest |
| `make check-sec` | Run `govulncheck` for security vulnerabilities |

## Docker

```bash
cd slippy-api
docker build -t slippy-api .
docker run -p 8080:8080 \
  -e SLIPPY_API_KEY=my-key \
  -e CLICKHOUSE_HOSTNAME=clickhouse.example.com \
  -e CLICKHOUSE_USERNAME=slippy \
  -e CLICKHOUSE_PASSWORD=secret \
  -e CLICKHOUSE_DATABASE=ci \
  slippy-api
```

## Key Design Decisions

- **Read-only**: The API only exposes read operations. The `SlipStoreAdapter` enforces this by adapting the upstream read+write `SlipStore` to the narrow `SlipReader` interface.
- **huma v2 + humago**: Code-first API framework with auto-generated OpenAPI 3.1 spec. Uses Go's standard library `net/http.ServeMux` via the humago adapter — no Gin, no Echo.
- **Bearer auth with constant-time comparison**: Prevents timing attacks. Operations without a `security` declaration (e.g., `/health`) pass through unauthenticated.
- **Cache decorator pattern**: `CachedSlipReader` wraps any `SlipReader` transparently. Caching is opt-in via environment variables and degrades gracefully if Dragonfly is unavailable.
- **OpenTelemetry**: Full SDK initialisation with traces and metrics via OTLP (gRPC or HTTP). Every layer creates properly-parented spans that waterfall correctly in a trace viewer:
  - **HTTP** — `otelhttp.NewHandler` creates the root request span
  - **Auth** — `auth.validateAPIKey` records scheme, operation, and outcome
  - **Handler** — `handler.*` spans capture operation parameters and results
  - **Cache** — `cache.*` spans show cache system, operation, and hit/miss status
  - **ClickHouse** — `clickhouse.*` spans record `db.system`, operation, and query parameters

  The SDK is configured entirely through standard `OTEL_*` environment variables.
- **Graceful shutdown**: `SIGINT`/`SIGTERM` triggers a 15-second graceful shutdown window.
- **No Vault**: All secrets are passed via environment variables, suitable for Kubernetes secret injection.

## Dependencies

| Dependency | Purpose |
|---|---|
| [huma/v2](https://github.com/danielgtaylor/huma) | REST API framework with OpenAPI 3.1 |
| [go-redis/v9](https://github.com/redis/go-redis) | Dragonfly/Redis client |
| [otelhttp](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp) | OpenTelemetry HTTP instrumentation |
| [otlptracegrpc / otlptracehttp](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace) | OTLP trace exporters (gRPC and HTTP) |
| [otlpmetricgrpc / otlpmetrichttp](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlpmetric) | OTLP metric exporters (gRPC and HTTP) |
| [goLibMyCarrier/slippy](https://github.com/MyCarrier-DevOps/goLibMyCarrier) | ClickHouse-backed routing slip store |
| [goLibMyCarrier/clickhouse](https://github.com/MyCarrier-DevOps/goLibMyCarrier) | ClickHouse configuration and connectivity |
| [testcontainers-go](https://github.com/testcontainers/testcontainers-go) | Container-based e2e testing (test only) |