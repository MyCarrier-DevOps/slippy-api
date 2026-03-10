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
| **Domain** | `internal/domain` | `SlipReader` / `ImageTagReader` / `CIJobLogReader` interfaces, type aliases for upstream `Slip`/`SlipWithCommit` |
| **Infrastructure** | `internal/infrastructure` | `SlipStoreAdapter` (read-only adapter), `ForkAwareSlipReader` (cross-repo fallback), `CachedSlipReader` (Dragonfly/Redis decorator), `BuildInfoReader` (image tag resolution), `CIJobLogStore` (CI job log queries) |
| **Handler** | `internal/handler` | HTTP route registration, request/response types, error mapping |
| **Middleware** | `internal/middleware` | Bearer token authentication with constant-time comparison |
| **Config** | `internal/config` | Environment variable loading and validation |
| **Main** | `main.go` | Wiring, server startup, graceful shutdown |

## API Endpoints

All endpoints except `/health`, `/openapi.json`, and `/docs` require a `Bearer` token in the `Authorization` header.

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check (no auth required). Returns `{"status":"ok"}` |
| `GET` | `/slips/{correlationID}` | Get a routing slip by its correlation ID |
| `GET` | `/slips/by-commit/{owner}/{repo}/{commitSHA}` | Get a routing slip by repository and commit SHA |
| `POST` | `/slips/find-by-commits` | Find the first matching slip for an ordered list of commits |
| `POST` | `/slips/find-all-by-commits` | Find all matching slips for a list of commits |
| `GET` | `/slips/{correlationID}/image-tags` | Resolve per-component image tags for a routing slip |
| `GET` | `/logs/{correlationID}` | Query CI job logs with cursor pagination and per-column filtering |
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

**GET /slips/{correlationID}/image-tags**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  https://slippy-api.example.com/slips/abc-123-def/image-tags
```

```json
{
  "build_scope": "modified",
  "tags": {
    "my-service": "26.10.a1b2c3d",
    "my-worker": "26.10.d4e5f6a"
  }
}
```

**GET /logs/{correlationID}**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  'https://slippy-api.example.com/logs/abc-123-def?limit=50&sort=desc&level=ERROR'
```

```json
{
  "items": [
    {
      "timestamp": "2026-03-10T12:00:00.123456789Z",
      "correlation_id": "abc-123-def",
      "level": "ERROR",
      "message": "build failed: exit code 1",
      "service": "ci-runner",
      "component": "build",
      "...": "..."
    }
  ],
  "next_page": "/logs/abc-123-def?limit=50&sort=desc&level=ERROR&cursor=2026-03-10T12%3A00%3A00.123456789Z%7C12345678901234",
  "count": 50
}
```

Supported query filters: `level`, `service`, `component`, `cluster`, `cloud`, `environment`, `namespace`, `message`, `ci_job_instance`, `ci_job_type`, `build_repository`, `build_image`, `build_branch`. Page size is controlled via `limit` (1–1000, default 100). Sort order via `sort` (`asc` or `desc`, default `desc`). Pagination uses an opaque `cursor` returned in `next_page`.

### Error Responses

| Status | Condition |
|---|---|
| `400` | Invalid correlation ID, invalid repository format, or invalid cursor |
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
| `SLIPPY_PIPELINE_CONFIG` | Pipeline configuration (file path or inline JSON) | `/config/pipeline.json` |
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
        │   ├── ci_job_log.go        # CIJobLogReader interface, log query/result types
        │   ├── image_tag.go         # ImageTagReader interface, ImageTagResult type
        │   └── slip.go              # SlipReader interface, type aliases
        ├── handler/
        │   ├── ci_job_log_handler.go     # GET /logs/{correlationID}
        │   ├── ci_job_log_handler_test.go
        │   ├── health.go                # GET /health
        │   ├── health_test.go
        │   ├── image_tag_handler.go      # GET /slips/{correlationID}/image-tags
        │   ├── image_tag_handler_test.go
        │   ├── slip_handler.go          # Slip CRUD routes + error mapping
        │   └── slip_handler_test.go
        ├── infrastructure/
        │   ├── buildinfo.go         # BuildInfoReader (image tags from ci.buildinfo)
        │   ├── buildinfo_test.go
        │   ├── cache.go             # CachedSlipReader (Dragonfly decorator)
        │   ├── cache_test.go
        │   ├── cijob.go             # CIJobLogStore (observability.ciJob queries)
        │   ├── cijob_test.go
        │   ├── fork_aware.go        # ForkAwareSlipReader (cross-repo fallback)
        │   ├── fork_aware_test.go
        │   ├── store.go             # SlipStoreAdapter (read-only adapter)
        │   └── store_test.go
        ├── middleware/
        │   ├── auth.go              # Bearer token auth middleware
        │   └── auth_test.go
        ├── telemetry/
        │   ├── telemetry.go         # OTel SDK init (traces + metrics)
        │   ├── telemetry_test.go
        │   └── testutil.go          # Shared test helpers for OTel
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
- **Fork-aware commit lookups**: `ForkAwareSlipReader` decorator handles forked repository queries by falling back to a commit-SHA-only lookup when the initial repository-scoped query returns `ErrSlipNotFound`, then retrying with the resolved repository name.
- **Cursor pagination with composite cursor**: The `/logs` endpoint uses a `timestamp|cityHash64` composite cursor to guarantee no data loss when multiple rows share the same nanosecond timestamp. Uses `LIMIT n+1` peek to determine next-page existence without a separate COUNT query.
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