# Project State — Slippy Application

> **Last Updated:** 2026-04-13
> **Status:** Write API implemented and end-to-end tested (ADO-80684); .dockerignore added; routing_slips write-back behaviour fully understood

## Overview

REST API for CI/CD routing slips. Provides read endpoints to query routing slips by correlation ID, commit SHA, and repository, plus write endpoints (v1 only) for slip creation and step lifecycle management. Backed by ClickHouse with optional Dragonfly/Redis caching and OpenTelemetry instrumentation. Supports GitHub-based commit ancestry resolution when a commit doesn't have a routing slip.

## Implemented Systems

### Core API — Read Endpoints (internal/handler, legacy + /v1)
- `GET /slips/{correlationID}` — Load slip by correlation ID
- `GET /slips/by-commit/{owner}/{repo}/{commitSHA}` — Load slip by repo + commit
- `POST /slips/find-by-commits` — Find first matching slip from commit list
- `POST /slips/find-all-by-commits` — Find all matching slips from commit list
- `GET /slips/{correlationID}/image-tags` — Image tag resolution via BuildInfoReader
- `GET /logs/{correlationID}` — CI job logs with cursor pagination, filtering, configurable sort
- `GET /health` — Health check

### Core API — Write Endpoints (internal/handler, /v1 only, requires `SLIPPY_WRITE_API_KEY`)
- `POST /v1/slips` — Create routing slip for push event (`CreateSlipForPush`)
- `POST /v1/slips/{correlationID}/steps/{stepName}/start` — Mark step as running
- `POST /v1/slips/{correlationID}/steps/{stepName}/complete` — Mark step as completed
- `POST /v1/slips/{correlationID}/steps/{stepName}/fail` — Mark step as failed
- `PUT /v1/slips/{correlationID}/components/{componentName}/image-tag` — Set component image tag

### Infrastructure (internal/infrastructure)
- **SlipStoreAdapter** — Adapts upstream `slippy.SlipStore` to read-only `domain.SlipReader`
- **SlipWriterAdapter** — Adapts upstream `*slippy.Client` to `domain.SlipWriter` (business-level write operations with OTel instrumentation)
- **SlipResolverAdapter** — Decorator that delegates all commit-based lookups (`LoadByCommit`, `FindByCommits`, `FindAllByCommits`) to `slippy.Client.ResolveSlip()` for ancestry resolution. Direct ClickHouse lookup is tried first; on `ErrSlipNotFound`, each commit is resolved via the library's ancestry walker.
- **CachedSlipReader** — Dragonfly/Redis caching decorator (passthrough, cache logic planned)
- **BuildInfoReader** — Resolves per-component image tags from ClickHouse ci.buildinfo
- **CIJobLogStore** — Queries `observability.ciJob` with cursor pagination, per-column filtering, composite cursor (`timestamp|cityHash64` tiebreaker)

### Domain (internal/domain)
- `slip.go` — `SlipReader`, `SlipWriter` interfaces + type aliases (`Slip`, `PushOptions`, `CreateSlipResult`, `StepStatus`, `StateHistoryEntry`, `AncestryEntry`, `ComponentDefinition`)
- `ci_job_log.go` — `CIJobLog`, `CIJobLogQuery`, `CIJobLogResult`, `CIJobLogReader` interface, `ErrInvalidCursor` sentinel, `SortOrder` type
- `image_tag.go` — `ImageTagReader` interface, `ImageTagResult`

### Middleware
- Two-key API authentication (`middleware/auth.go`) — `SLIPPY_API_KEY` (read), `SLIPPY_WRITE_API_KEY` (read+write)

### Telemetry
- OpenTelemetry tracing and metrics (`telemetry/telemetry.go`)

### Configuration
- Environment variable-based config (`config/config.go`)

## Recent Changes

### 2026-04-13: End-to-end testing, .dockerignore, routing_slips write-back analysis

**End-to-end test script** (`slippy-api/scripts/test-script.sh`):
- Full pipeline simulation: create slip → unit_tests → secret_scan → builds (api+worker) → dev_deploy → dev_tests → hydrate trigger → final read-back
- Step names confirmed from `workflow-core/workflows/templates/slip-routed/*.yaml`:
  - `builds` (aggregate, with `component_name`), `unit_tests`, `secret_scan`, `dev_deploy`, `dev_tests`
- Hydrate hack: re-completes `builds/api` after all steps to trigger aggregate write-back, flushing all `*_status` columns to `routing_slips`

**`slippy-api/.dockerignore` added:**
- Excludes `.env`/`.env.*`, `coverage.out`, `*.test`, `scripts/`, IDE dirs, `.git/`, `.github/`, `docs/`, `*.md`

**routing_slips write-back behaviour (confirmed from goLibMyCarrier/slippy source):**
- Pure pipeline steps (`unit_tests`, `secret_scan`, `dev_deploy`, `dev_tests`) write only to `slip_component_states` event log + `state_history` JSON via `AppendHistory`
- `*_status` columns in `routing_slips` are updated only when aggregate steps (`builds`) trigger `updateAggregateStatusFromComponentStates` → `Load()` → `hydrateSlip()` → full `Update()`
- `hydrateSlip` reads all steps from `slip_component_states` → all `*_status` columns are refreshed atomically in the same write-back
- `GET /slips/{id}` always calls `hydrateSlip` in memory → always authoritative regardless of `routing_slips` column state
- Dashboard query on `routing_slips.*_status` is reliable for abandoned/completed slips (abandonment triggers a full write-back) but lags for in-progress slips with no post-step aggregate activity

### 2026-04-10: ADO-80684 — SlipWriter Interface (Write API)
**Feature:** Expanded slippy-api from read-only to read+write by adding 5 business-level write endpoints backed by `slippy.Client`.

**Write endpoints (v1 only, require `SLIPPY_WRITE_API_KEY`):**
- `POST /v1/slips` — Create slip for push event (includes ancestry resolution)
- `POST /v1/slips/{correlationID}/steps/{stepName}/start` — Mark step as running
- `POST /v1/slips/{correlationID}/steps/{stepName}/complete` — Mark step as completed
- `POST /v1/slips/{correlationID}/steps/{stepName}/fail` — Mark step as failed
- `PUT /v1/slips/{correlationID}/components/{componentName}/image-tag` — Set image tag

**Two-key auth:**
- `SLIPPY_API_KEY` — read endpoints only
- `SLIPPY_WRITE_API_KEY` (optional) — read + write (superset). When absent, server runs read-only.

**Key decisions:**
- Wraps `*slippy.Client` (business-level) not `slippy.SlipStore` (raw) — avoids reimplementing ancestry resolution, atomic step+history writes, pipeline config lookup
- Write routes registered on `/v1` only (no legacy unversioned paths)
- `PipelineConfig` passed to `slippy.Client` for `SetComponentImageTag` and `CreateSlipForPush`
- `SLIPPY_SKIP_MIGRATIONS` env var (default: `true`) replaces hardcoded `SkipMigrations: true`
- Adversarial review caught 4 critical issues pre-implementation (PipelineConfig missing, `[]error` JSON serialization, `ComponentDefinition` no JSON tags, auth scheme detection)

**Files created:** `slip_writer.go`, `slip_writer_test.go`, `slip_write_handler.go`, `slip_write_handler_test.go`
**Files modified:** `domain/slip.go`, `config/config.go`, `middleware/auth.go`, `main.go`, plus tests
**OpenAPI spec + Go client regenerated** with all 5 write endpoints

**Coverage:** config 100%, handler 100%, infrastructure 97.4%, middleware 97.9%. Lint: 0 issues.

### 2026-04-08: Bump goLibMyCarrier to v1.3.72
Updated all goLibMyCarrier submodules (`clickhouse`, `logger`, `slippy`, `clickhousemigrator`, `github`) from v1.3.71 → v1.3.72 on branch `chore/goLibMyCarrier-1.3.72`. `go mod tidy` run. All checks pass: fmt clean, lint 0 issues, tests green (97.1% infra coverage), build OK.

### 2026-03-11: Removed ForkAwareSlipReader, Ancestry on All Commit Lookups
**Problem:** `ForkAwareSlipReader` intercepted `ErrSlipNotFound` and attempted cross-repo resolution via a ClickHouse commit-SHA-only query. This was unnecessary — routing slips already store the correct repository name — and it actively interfered with ancestry resolution on the `FindByCommits` path (returning 404 instead of letting ancestry resolve).

**Solution:**
1. Deleted `fork_aware.go` and `fork_aware_test.go` entirely
2. Extended `SlipResolverAdapter` to perform ancestry resolution on `FindByCommits` and `FindAllByCommits` (previously passthroughs)
3. Simplified decorator chain: `SlipStoreAdapter` → `SlipResolverAdapter` → `CachedSlipReader`
4. `LoadByCommit` now returns `ErrSlipNotFound` directly when ancestry fails (no fallback to reader chain)

**How `FindByCommits` ancestry works:**
- Direct ClickHouse lookup first via `reader.FindByCommits()`
- On `ErrSlipNotFound`, iterates each commit calling `resolver.ResolveSlip()` (ancestry + image tag fallback)
- Returns the first resolved slip with the input commit as `matched_commit`
- Non-not-found errors short-circuit immediately

**Files changed:**
- Deleted: `internal/infrastructure/fork_aware.go`, `internal/infrastructure/fork_aware_test.go`
- `internal/infrastructure/ancestry.go` — `FindByCommits` and `FindAllByCommits` now use ancestry fallback
- `internal/infrastructure/ancestry_test.go` — 18 tests (100% coverage on ancestry.go), mock reader moved here
- `main.go` — Removed fork-aware wiring, `SlipResolverAdapter` wraps `adapter` directly

**Verified end-to-end:** `POST /slips/find-by-commits` with commit `e7b8469f` now resolves via ancestry to ancestor `6e81828` (correlation ID `7f6258ff`).

### 2026-03-11: GitHub Env Vars Required
`SLIPPY_GITHUB_APP_ID` and `SLIPPY_GITHUB_APP_PRIVATE_KEY` are now required — the server refuses to start without them. `GitHubEnabled()` removed from config.

**Environment variables (required):**
- `SLIPPY_GITHUB_APP_ID` — GitHub App ID
- `SLIPPY_GITHUB_APP_PRIVATE_KEY` — PEM-encoded private key or file path

**Environment variables (optional):**
- `SLIPPY_GITHUB_ENTERPRISE_URL` — GitHub Enterprise base URL
- `SLIPPY_ANCESTRY_DEPTH` — How many commits to walk (default: 25)

### 2026-03-10: CI Job Logs Endpoint
**Feature:** Added `GET /logs/{correlationID}` with:
- Cursor-based pagination (max 1000 page size, next-page as full URL)
- Composite cursor (`timestamp|cityHash64`) to prevent data loss on timestamp ties
- Per-column filtering (level, service, component, cluster, cloud, environment, namespace, message, ci_job_instance, ci_job_type, build_repository, build_image, build_branch)
- Configurable sort order (asc/desc)

**Files:**
- `internal/domain/ci_job_log.go` — Domain types and interfaces
- `internal/infrastructure/cijob.go` — ClickHouse store with composite cursor
- `internal/infrastructure/cijob_test.go` — 13 unit tests
- `internal/handler/ci_job_log_handler.go` — HTTP handler with huma validation
- `internal/handler/ci_job_log_handler_test.go` — 9 handler tests
- `main.go` — Wiring

### 2026-03-10: Devil's Advocate Review Fixes
Adversarial review identified 6 issues; 4 required code changes:
1. **fix(logs):** Composite cursor with `cityHash64` tiebreaker to prevent silent data loss on timestamp ties (`f6bfc30`)
2. **fix(auth):** `writeError` logs failures instead of setting unreachable 500 status (`8e013c6`)
3. **fix(cache):** Removed dead `cacheKey` function, `var _ = cacheKey`, and unused `strings` import (`ea53084`)
4. **fix(store):** Eliminated named-return `err` shadowing in `queryBuildScope`/`queryBuildInfo` (`4e7f6ec`)
5. **test(handler):** Strengthened URL encoding assertion to verify full encoded form (`60a1141`)
6. `rows.Err()` check in `queryBuildScope` was already present — no change needed

### 2026-03-10: Lint Cleanup
- Fixed errcheck on deferred `rows.Close()` / `tp.Shutdown()` calls
- Added named returns where required by gocritic/unnamedResult
- Removed unused `cacheKey` function
- Applied golines formatting (120 char max)

### 2026-03-10: Fork-Aware Commit Lookups
Added `ForkAwareSlipReader` decorator that resolves forked repository commit lookups by querying ClickHouse by commit SHA without repository filter, then retrying with the resolved repo name.

## Current Focus

ADO-80684 (SlipWriter) implemented — pending PR review and merge.

## Architectural Decisions

- **Decorator pattern**: Ancestry resolution, caching, and store each in separate decorators maintaining single-responsibility
- **Ancestry resolution**: Delegates to `slippy.Client.ResolveSlip()` via `SlipResolverAdapter`. All commit-based lookups (`LoadByCommit`, `FindByCommits`, `FindAllByCommits`) try direct ClickHouse first, then fall back to ancestry walking. Resolution logic is maintained in the `slippy` library, not reimplemented locally.
- **GitHub App auth**: Uses `ghinstallation/v2` for JWT-based GitHub App authentication with per-org installation caching (handled by `goLibMyCarrier/github.GraphQLClient`)
- **Composite cursor pagination**: `timestamp|cityHash64(row_data)` prevents data loss when multiple rows share the same nanosecond timestamp
- **LIMIT n+1 peek**: Request one extra row to determine if a next page exists without a separate COUNT query
- **Named parameters in ClickHouse**: `{name:Type}` syntax for dynamic filter injection
- **No fork-aware decorator**: Routing slips store the correct repository name; cross-repo fallback was unnecessary and interfered with ancestry resolution

## Technical Debt / Known Issues

- CachedSlipReader is passthrough only — actual caching not yet implemented
- E2E tests require Docker/testcontainers (skipped in CI without Docker)

## Next Steps (Not Yet Implemented)

- Implement actual caching logic in CachedSlipReader
- PR review and merge of `feat/log-search` branch
