# Slippy-API: Architecture, Functions & Flow

Go REST API for CI/CD routing slips backed by ClickHouse.

---

## 1. Project Structure

```
slippy-api/
├── .github/
│   ├── .golangci.yml                    # Linter config (golangci-lint v2)
│   ├── Gitversion.yml                   # Semantic versioning config
│   └── workflows/ci.yaml               # CI/CD pipeline
├── docs/                                # Documentation
├── makefile                             # Build, test, lint, format targets
├── README.md / CONTRIBUTING.md
├── slippy-api/                          # Main Go module
│   ├── Dockerfile                       # Multi-stage Docker build
│   ├── go.mod / go.sum                  # Go 1.26, module: github.com/MyCarrier-DevOps/slippy-api
│   ├── main.go                          # Entry point and dependency wiring
│   ├── main_test.go                     # Integration tests for the HTTP server
│   ├── api/v1/
│   │   ├── openapi.json                 # Generated OpenAPI 3.1 spec
│   │   └── openapi-v1.json              # Versioned spec
│   └── internal/
│       ├── config/config.go             # Env-based configuration loading
│       ├── domain/                      # Interfaces and types (innermost layer)
│       │   ├── slip.go                  # SlipReader interface
│       │   ├── image_tag.go             # ImageTagReader interface
│       │   └── ci_job_log.go            # CIJobLogReader interface
│       ├── infrastructure/              # Adapters and external service integrations
│       │   ├── store.go                 # SlipStoreAdapter (read-only ClickHouse wrapper)
│       │   ├── ancestry.go              # SlipResolverAdapter (GitHub ancestry walk)
│       │   ├── cache.go                 # CachedSlipReader (Redis/Dragonfly decorator)
│       │   ├── buildinfo.go             # BuildInfoReader (image tag resolution)
│       │   └── cijob.go                 # CIJobLogStore (CI job log queries)
│       ├── handler/                     # HTTP handlers (huma routes)
│       │   ├── health.go               # GET /health
│       │   ├── slip_handler.go          # Slip query routes (4 operations)
│       │   ├── image_tag_handler.go     # Image tag resolution route
│       │   └── ci_job_log_handler.go    # CI job log route with pagination
│       ├── middleware/auth.go           # Bearer token authentication
│       ├── telemetry/telemetry.go       # OpenTelemetry SDK init
│       └── e2e/e2e_test.go             # End-to-end tests with testcontainers
└── slippy-client/                       # Auto-generated Go client library
    ├── go.mod
    ├── oapi-codegen.yaml
    └── client.gen.go
```

---

## 2. Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Go | 1.26 |
| HTTP Framework | Huma v2 (OpenAPI 3.1 native) | v2.37.3 |
| Database | ClickHouse | clickhouse-go v2.44.0 |
| Cache | Dragonfly / Redis (optional) | go-redis v9.18.0 |
| Observability | OpenTelemetry (traces + metrics) | v1.43.0 |
| GitHub Integration | GitHub App auth (GraphQL) | goLibMyCarrier |
| Internal Libraries | goLibMyCarrier (clickhouse, slippy, logger, github) | v1.3.72 |

---

## 3. API Endpoints

Read endpoints require `SLIPPY_API_KEY` (or `SLIPPY_WRITE_API_KEY`). Write endpoints require `SLIPPY_WRITE_API_KEY` only. Public endpoints (`/health`, `/docs`, `/openapi.json`) require no auth.

### Read Endpoints (legacy + /v1)

| Method | Path | OperationID | Handler | Description |
|--------|------|-------------|---------|-------------|
| `GET` | `/health` | `health-check` | `handler/health.go` | Returns `{"status":"ok"}` |
| `GET` | `/slips/{correlationID}` | `get-slip` | `SlipHandler.getSlip()` | Load routing slip by correlation ID |
| `GET` | `/slips/by-commit/{owner}/{repo}/{commitSHA}` | `get-slip-by-commit` | `SlipHandler.getSlipByCommit()` | Get slip by repo + commit; walks ancestry if not found |
| `POST` | `/slips/find-by-commits` | `find-by-commits` | `SlipHandler.findByCommits()` | Find first matching slip for a list of commits |
| `POST` | `/slips/find-all-by-commits` | `find-all-by-commits` | `SlipHandler.findAllByCommits()` | Find all matching slips for a list of commits |
| `GET` | `/slips/{correlationID}/image-tags` | `get-image-tags` | `ImageTagHandler.getImageTags()` | Resolve per-component image tags |
| `GET` | `/logs/{correlationID}` | `get-logs` | `CIJobLogHandler.getLogs()` | Query CI job logs (cursor-paginated, filterable) |
| `GET` | `/openapi.json` | — | Auto-generated | OpenAPI 3.1 spec |
| `GET` | `/docs` | — | Stoplight Elements | Interactive API docs |

### Write Endpoints (/v1 only)

| Method | Path | OperationID | Handler | Description |
|--------|------|-------------|---------|-------------|
| `POST` | `/v1/slips` | `create-slip` | `SlipWriteHandler.createSlip()` | Create routing slip for a push event |
| `POST` | `/v1/slips/{correlationID}/steps/{stepName}/start` | `start-step` | `SlipWriteHandler.startStep()` | Mark step as running |
| `POST` | `/v1/slips/{correlationID}/steps/{stepName}/complete` | `complete-step` | `SlipWriteHandler.completeStep()` | Mark step as completed |
| `POST` | `/v1/slips/{correlationID}/steps/{stepName}/fail` | `fail-step` | `SlipWriteHandler.failStep()` | Mark step as failed |
| `PUT` | `/v1/slips/{correlationID}/components/{componentName}/image-tag` | `set-image-tag` | `SlipWriteHandler.setImageTag()` | Record built image tag for a component |

---

## 4. Domain Interfaces

### SlipReader (`internal/domain/slip.go`)

```go
type SlipReader interface {
    Load(ctx, correlationID string) (*Slip, error)
    LoadByCommit(ctx, repository, commitSHA string) (*Slip, error)
    FindByCommits(ctx, repository string, commits []string) (*Slip, matchedCommit string, error)
    FindAllByCommits(ctx, repository string, commits []string) ([]SlipWithCommit, error)
}
```

- `Slip` is a type alias to `slippy.Slip` from the upstream goLibMyCarrier library
- Contains fields: CorrelationID, CommitSHA, Repository, Branch, CreatedAt, Status, FailedStep, etc.

### SlipWriter (`internal/domain/slip.go`)

```go
type SlipWriter interface {
    CreateSlipForPush(ctx, opts PushOptions) (*CreateSlipResult, error)
    StartStep(ctx, correlationID, stepName, componentName string) error
    CompleteStep(ctx, correlationID, stepName, componentName string) error
    FailStep(ctx, correlationID, stepName, componentName, reason string) error
    SetComponentImageTag(ctx, correlationID, componentName, imageTag string) error
}
```

- Maps to business-level operations from `slippy.Client` (not raw store methods)
- Used by pushhookparser (slip creation) and Slippy CI CLI (pre-job/post-job step lifecycle)
- All methods are synchronous, non-blocking ClickHouse writes

### ImageTagReader (`internal/domain/image_tag.go`)

```go
type ImageTagReader interface {
    ResolveImageTags(ctx, correlationID string) (*ImageTagResult, error)
}
```

Returns `ImageTagResult`:
- `Tags map[string]string` — component name to image tag
- `BuildScope string` — `"all"` or `"modified"`
- `SlipTag string` — computed tag in `YY.WW.SHA7` format (e.g., `"26.09.aef1234"`)

### CIJobLogReader (`internal/domain/ci_job_log.go`)

```go
type CIJobLogReader interface {
    QueryLogs(ctx, query *CIJobLogQuery) (*CIJobLogResult, error)
}
```

- Supports cursor-based pagination with `timestamp|cityHash64` composite cursors
- 13 filter columns: Level, Service, Component, Cluster, Cloud, Environment, Namespace, Message, CIJobInstance, CIJobType, BuildRepository, BuildImage, BuildBranch
- Configurable sort order (asc/desc) and limit (1–1000, default 100)

---

## 5. Infrastructure Implementations

### SlipStoreAdapter (`internal/infrastructure/store.go`)

- **Wraps**: `slippy.SlipStore` (upstream read+write store)
- **Purpose**: Enforces read-only interface — only exposes `SlipReader` methods
- **Backend**: ClickHouse via `slippy.NewClickHouseStoreFromConfig()`
- **Tracing**: Every method creates an OTel span under `slippy-api/store`
- **Compile-time check**: `var _ domain.SlipReader = (*SlipStoreAdapter)(nil)`

### SlipResolverAdapter (`internal/infrastructure/ancestry.go`)

- **Wraps**: `SlipReader` + `slippy.Client` (GitHub)
- **Purpose**: Decorates slip lookups with commit ancestry resolution
- **Flow**: Try direct ClickHouse lookup → if not found, walk commit ancestry via GitHub GraphQL → retry lookups on ancestor commits
- **Config**: `SLIPPY_ANCESTRY_DEPTH` (default: 25 commits deep)
- **Tracing**: OTel span under `slippy-api/ancestry`

### CachedSlipReader (`internal/infrastructure/cache.go`)

- **Wraps**: Any `SlipReader`
- **Purpose**: Optional Redis/Dragonfly caching decorator (currently passthrough, full caching planned)
- **Config**: `DRAGONFLY_HOST`, `CACHE_TTL` (default: 10m)
- **Tracing**: OTel span under `slippy-api/cache`

### BuildInfoReader (`internal/infrastructure/buildinfo.go`)

- **Implements**: `ImageTagReader`
- **Queries**: `ci.repoproperties` for build_scope, `ci.buildinfo` for per-component tags
- **Tag format**: `YY.WW.SHA7` (e.g., `"26.09.aef1234"`)
- **Logic**:
  - `build_scope=all` → all components share the slip-computed tag
  - `build_scope=modified` → each component carries its actual tag from buildinfo
- **Tracing**: OTel span under `slippy-api/buildinfo`

### SlipWriterAdapter (`internal/infrastructure/slip_writer.go`)

- **Wraps**: `*slippy.Client` (high-level business client, not raw `SlipStore`)
- **Purpose**: Implements `domain.SlipWriter` — delegates to `slippy.Client` methods that include business logic (ancestry resolution, atomic step+history writes, pipeline config lookup)
- **Write path**: Bypasses the read decorator chain (cache, ancestry resolver) — writes go directly through the client to ClickHouse
- **Tracing**: OTel spans under `slippy-api/writer`
- **Compile-time check**: `var _ domain.SlipWriter = (*SlipWriterAdapter)(nil)`

### CIJobLogStore (`internal/infrastructure/cijob.go`)

- **Implements**: `CIJobLogReader`
- **Queries**: `observability.ciJob` table in ClickHouse
- **Pagination**: Cursor = `RFC3339Nano|hash`, supports ascending/descending sort
- **Filters**: 13 column-level exact-match filters
- **Tracing**: OTel span under `slippy-api/cijob`

---

## 6. Request Flow

```
                         HTTP Request
                              │
                    OTel HTTP Instrumentation
                              │
                    Auth Middleware (two-key)
                    apiKey=read, writeApiKey=read+write
                              │
                        Huma Router
                              │
         ┌────────┬───────────┼──────────┬──────────┐
         │        │           │          │          │
    SlipHandler  SlipWrite  ImageTag  CIJobLog   Health
    (4 read ops) Handler    Handler   Handler
         │       (5 write    │          │
         │        ops)       │          │
         │        │          │          │
  ┌──────┘   ┌────┘          │          │
  │ SlipReader│ SlipWriter   │          │
  │          │               │          │
  ▼          ▼               ▼          ▼
CachedSlip  SlipWriter   BuildInfo   CIJobLog
Reader      Adapter      Reader      Store
  │        (*slippy.      │          │
  ▼         Client)       │          │
SlipResolver    │         │          │
Adapter         │         │          │
  │             │         │          │
  ▼             │         │          │
SlipStore       │         │          │
Adapter         │         │          │
  │             │         │          │
  └─────────────┴─────────┴──────────┘
                │
           ClickHouse
     (ci.*, observability.ciJob)
```

**Read path**: Handler → CachedSlipReader → SlipResolverAdapter → SlipStoreAdapter → ClickHouse

**Write path**: Handler → SlipWriterAdapter → slippy.Client → ClickHouse (bypasses cache/ancestry decorators)

---

## 7. Startup Flow (`main.go`)

```
main() → run()
  │
  ├─ 1. telemetry.Init()          — OpenTelemetry SDK (traces + metrics)
  ├─ 2. config.Load()             — Environment variables → Config struct
  ├─ 3. slippy.LoadPipelineConfig — Pipeline configuration
  ├─ 4. slippy.NewClickHouseStoreFromConfig — ClickHouse (cfg.SkipMigrations)
  ├─ 5. slippy.NewGitHubClient    — GitHub App client for ancestry resolution
  ├─ 6. slippy.NewClientWithDependencies — slippy.Client (with PipelineConfig)
  ├─ 7. connectCache()            — Optional Redis/Dragonfly (graceful fallback)
  ├─ 8. SlipWriterAdapter          — Optional (when SLIPPY_WRITE_API_KEY set)
  ├─ 9. buildHandler()            — Wire all components:
  │     ├─ SlipStoreAdapter(store)
  │     ├─ CachedSlipReader(SlipResolverAdapter(storeAdapter, slippyClient))
  │     ├─ BuildInfoReader(session, reader)
  │     ├─ CIJobLogStore(session)
  │     ├─ Auth middleware(readKey, writeKey)
  │     ├─ Register read routes on legacy + /v1 group
  │     └─ Register write routes on /v1 only group (if writer != nil)
  └─ 10. HTTP server with graceful shutdown (SIGINT/SIGTERM, 15s timeout)
```

---

## 8. Authentication

**File**: `internal/middleware/auth.go`

- **Scheme**: Two-key Bearer token in `Authorization` header
- **Keys**:
  - `SLIPPY_API_KEY` — grants access to read endpoints only
  - `SLIPPY_WRITE_API_KEY` (optional) — grants access to both read and write endpoints (superset)
- **Validation**: Constant-time comparison (`subtle.ConstantTimeCompare`) to prevent timing attacks
- **Security scheme detection**: Middleware inspects `ctx.Operation().Security` map keys to distinguish `apiKey` (read) from `writeApiKey` (write) operations
- **Behavior**:
  - Operations without security requirements skip auth (`/health`, `/docs`, `/openapi.json`)
  - Write operations (`writeApiKey` security): only `SLIPPY_WRITE_API_KEY` accepted
  - Read operations (`apiKey` security): either key accepted
  - Missing/malformed token → `401 Unauthorized`
  - Invalid token → `403 Forbidden`
- **OTel**: Span with `auth.result` and `auth.access_level` (`"read"` or `"write"`) attributes

---

## 9. Error Handling

| Domain Error | HTTP Status | Endpoints | Description |
|-------------|-------------|-----------|-------------|
| `slippy.ErrSlipNotFound` | 404 Not Found | Read + Write | Routing slip does not exist |
| `slippy.ErrInvalidCorrelationID` | 400 Bad Request | Read + Write | Malformed correlation ID |
| `slippy.ErrInvalidRepository` | 400 Bad Request | Read + Write | Malformed repository string |
| `slippy.ErrInvalidConfiguration` | 400 Bad Request | Write | Invalid pipeline configuration |
| `"invalid push options: ..."` | 400 Bad Request | Write | Missing required fields in create slip request |
| `*slippy.StepError` | 422 Unprocessable Entity | Write | Step operation failed (e.g., invalid step name) |
| `*slippy.SlipError` | 422 Unprocessable Entity | Write | Slip operation failed |
| `domain.ErrInvalidCursor` | 400 Bad Request | Read | Malformed pagination cursor |
| Generic error | 500 Internal Server Error | Read + Write | Unexpected server error |

OTel spans distinguish client errors (not-found, invalid input → `Unset` status) from server errors (`Error` status).

---

## 10. Configuration (Environment Variables)

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `SLIPPY_API_KEY` | Yes | — | Bearer token for read endpoint authentication |
| `SLIPPY_WRITE_API_KEY` | No | — | Bearer token for write endpoints (enables write routes when set) |
| `PORT` | No | 8080 | HTTP server port |
| `SLIPPY_GITHUB_APP_ID` | Yes | — | GitHub App ID |
| `SLIPPY_GITHUB_APP_PRIVATE_KEY` | Yes | — | GitHub App private key (PEM or file path) |
| `SLIPPY_GITHUB_ENTERPRISE_URL` | No | — | GitHub Enterprise Server URL |
| `SLIPPY_ANCESTRY_DEPTH` | No | 25 | Max commit ancestry walk depth |
| `CLICKHOUSE_HOSTNAME` | Yes | — | ClickHouse server host |
| `CLICKHOUSE_USERNAME` | Yes | — | ClickHouse auth |
| `CLICKHOUSE_PASSWORD` | Yes | — | ClickHouse auth |
| `CLICKHOUSE_DATABASE` | No | ci | ClickHouse database |
| `CLICKHOUSE_PORT` | No | 9000 | ClickHouse native port |
| `K8S_NAMESPACE` | No | — | Selects `ci_test` or `ci_dev` database |
| `DRAGONFLY_HOST` | No | — | Enables caching when set |
| `DRAGONFLY_PORT` | No | 6379 | Cache port |
| `DRAGONFLY_PASSWORD` | No | — | Cache auth |
| `CACHE_TTL` | No | 10m | Cache TTL (Go duration) |
| `SLIPPY_SKIP_MIGRATIONS` | No | true | Set to `false` to enable ClickHouse schema migrations at startup |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | No | — | Enables OTel when set |
| `OTEL_SERVICE_NAME` | No | slippy-api | OTel service name |

---

## 11. Observability (OpenTelemetry)

**Tracer scopes**:
- `slippy-api/handler` — HTTP handler operations
- `slippy-api/auth` — authentication middleware
- `slippy-api/store` — ClickHouse slip store operations
- `slippy-api/cache` — Redis/Dragonfly cache operations
- `slippy-api/ancestry` — GitHub commit ancestry resolution
- `slippy-api/buildinfo` — image tag resolution queries
- `slippy-api/cijob` — CI job log queries
- `slippy-api/writer` — write operations (create slip, step updates, image tags)

**Export**: gRPC (default) or HTTP/protobuf, configurable via `OTEL_EXPORTER_OTLP_PROTOCOL`

**Propagation**: W3C Trace Context + Baggage

**Fallback**: No-op provider if SDK disabled or endpoint unreachable

---

## 12. Testing Patterns

| Pattern | Location | Description |
|---------|----------|-------------|
| Unit tests (mocks) | `*_test.go` in handler/ and infrastructure/ | Function-pointer mocks for each interface |
| Compile-time checks | All adapter files | `var _ Interface = (*Impl)(nil)` |
| Table-driven tests | Throughout | Multiple scenarios per test function |
| HTTP handler tests | `main_test.go` | `httptest.NewRequest` / `httptest.NewRecorder` |
| E2E tests | `e2e/e2e_test.go` | Full stack with `testcontainers-go` (Redis) |
| Coverage threshold | CI pipeline | 80% minimum |

---

## 13. Build & Deploy

- **Docker**: Multi-stage build (Go 1.26 builder → Alpine 3.23 runtime), `CGO_ENABLED=0`, port 8080
- **CI**: GitHub Actions — unit tests, lint, vuln scan, client generation, client release
- **Versioning**: GitVersion semantic versioning
- **Client auto-generation**: OpenAPI spec → oapi-codegen → `slippy-client/client.gen.go`

---

## 14. Architecture Patterns Summary

| Pattern | Where | Purpose |
|---------|-------|---------|
| **Adapter** | `SlipStoreAdapter`, `SlipWriterAdapter` | Enforce domain interfaces on upstream types |
| **Decorator** | `CachedSlipReader`, `SlipResolverAdapter` | Layer caching and ancestry resolution transparently |
| **Two-key Auth** | `middleware/auth.go` | Separate read (`apiKey`) and write (`writeApiKey`) security schemes |
| **Dependency Injection** | All handlers | Receive interfaces, not implementations; wired in `main.go` |
| **Clean Architecture** | `domain/` → `infrastructure/` → `handler/` | Inner layers have no knowledge of outer layers |
| **Cursor Pagination** | `CIJobLogStore` | `timestamp\|cityHash64` composite cursor for stable paging |
| **Graceful Shutdown** | `main.go` | SIGINT/SIGTERM with 15-second drain window |
