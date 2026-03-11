# Project State — Slippy Application

> **Last Updated:** 2026-03-11
> **Status:** Ancestry resolution refactored to delegate to slippy library's ResolveSlip()

## Overview

Read-only API for CI/CD routing slips. Provides endpoints to query routing slips by correlation ID, commit SHA, and repository. Backed by ClickHouse with optional Dragonfly/Redis caching and OpenTelemetry instrumentation. Supports GitHub-based commit ancestry resolution when a commit doesn't have a routing slip.

## Implemented Systems

### Core API (internal/handler)
- `GET /slips/{correlationID}` — Load slip by correlation ID
- `GET /slips/by-commit/{owner}/{repo}/{commitSHA}` — Load slip by repo + commit
- `POST /slips/find-by-commits` — Find first matching slip from commit list
- `POST /slips/find-all-by-commits` — Find all matching slips from commit list
- `GET /logs/{correlationID}` — CI job logs with cursor pagination, filtering, configurable sort
- `GET /health` — Health check
- Image tag resolution via BuildInfoReader

### Infrastructure (internal/infrastructure)
- **SlipStoreAdapter** — Adapts upstream `slippy.SlipStore` to read-only `domain.SlipReader`
- **ForkAwareSlipReader** — Decorator that handles forked repository commit lookups
- **SlipResolverAdapter** — Thin adapter that delegates `LoadByCommit` to `slippy.Client.ResolveSlip()` (commit ancestry + image tag fallback), with fork-aware fallback for repo name mismatches. Other methods pass through to the reader chain.
- **CachedSlipReader** — Dragonfly/Redis caching decorator (passthrough, cache logic planned)
- **BuildInfoReader** — Resolves per-component image tags from ClickHouse ci.buildinfo
- **CIJobLogStore** — Queries `observability.ciJob` with cursor pagination, per-column filtering, composite cursor (`timestamp|cityHash64` tiebreaker)

### Domain (internal/domain)
- `ci_job_log.go` — `CIJobLog`, `CIJobLogQuery`, `CIJobLogResult`, `CIJobLogReader` interface, `ErrInvalidCursor` sentinel, `SortOrder` type

### Middleware
- API key authentication (`middleware/auth.go`)

### Telemetry
- OpenTelemetry tracing and metrics (`telemetry/telemetry.go`)

### Configuration
- Environment variable-based config (`config/config.go`)

## Recent Changes

### 2026-03-11: Refactored Ancestry Resolution to Library Delegation
**Problem:** Initial ancestry implementation reimplemented `slippy.Client.ResolveSlip()` logic locally, creating a maintenance problem — any changes to the resolution algorithm in the library would need to be manually replicated.

**Solution:** Replaced `AncestryAwareSlipReader` (reimplemented ancestry logic) with `SlipResolverAdapter` (thin adapter delegating to `slippy.Client.ResolveSlip()`).

**Architecture:**
- Decorator chain: `SlipStoreAdapter` → `ForkAwareSlipReader` → `SlipResolverAdapter` → `CachedSlipReader` → handler
- `SlipResolverAdapter.LoadByCommit` → `slippy.Client.ResolveSlip()` (ancestry + image tag fallback)
- On `ErrSlipNotFound`, falls back to reader chain (fork-aware) for repo name mismatches
- `FindByCommits`, `FindAllByCommits`, `Load` → delegate to reader chain directly
- `SlipResolver` interface defined for testability (mocking `slippy.Client`)

**Files changed:**
- `internal/infrastructure/ancestry.go` — New `SlipResolver` interface, `SlipResolverAdapter` (was `AncestryAwareSlipReader`)
- `internal/infrastructure/ancestry_test.go` — 9 tests using mock `SlipResolver` (was 12 tests with mock GitHub API)
- `main.go` — Creates `slippy.NewClientWithDependencies(store, ghClient, config)` and wraps with `NewSlipResolverAdapter`

**Key benefit:** Resolution logic maintained in one place (the `slippy` library). This API just delegates.

### 2026-03-11: GitHub Commit Ancestry Resolution (Initial Implementation)
**Root cause fix:** When a commit SHA doesn't have a routing slip in ClickHouse (not all commits generate slips), the API now uses the GitHub GraphQL API to walk backwards through the commit history to find an ancestor commit that does have a routing slip. This follows the same pattern as the upstream `slippy` library's `ResolveSlip()`.

**Architecture:**
- Decorator chain: `SlipStoreAdapter` → `ForkAwareSlipReader` → `AncestryAwareSlipReader` → `CachedSlipReader` → handler
- `AncestryAwareSlipReader` intercepts `LoadByCommit` failures, calls `GitHubAPI.GetCommitAncestry()`, then uses `FindByCommits` with the ancestor list
- `FindByCommits` and `FindAllByCommits` pass through (caller already provides commit list)
- GitHub config is optional — if not provided, ancestry resolution is disabled gracefully

**Files:**
- `internal/infrastructure/ancestry.go` — `AncestryAwareSlipReader` decorator, `GitHubAPI` interface
- `internal/infrastructure/ancestry_test.go` — 12 unit tests (100% coverage)
- `internal/config/config.go` — Added `GitHubAppID`, `GitHubPrivateKey`, `GitHubEnterpriseURL`, `AncestryDepth` fields
- `internal/config/config_test.go` — 7 new tests for GitHub config (100% coverage)
- `main.go` — Wired GitHub client via `slippy.NewGitHubClient()`, added `AncestryAwareSlipReader` to decorator chain

**Environment variables:**
- `SLIPPY_GITHUB_APP_ID` — GitHub App ID (optional, enables ancestry resolution)
- `SLIPPY_GITHUB_APP_PRIVATE_KEY` — PEM-encoded private key or file path
- `SLIPPY_GITHUB_ENTERPRISE_URL` — GitHub Enterprise base URL (optional)
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

Ancestry resolution delegates to `slippy.Client.ResolveSlip()`. Decorator chain: `SlipStoreAdapter` → `ForkAwareSlipReader` → `SlipResolverAdapter` → `CachedSlipReader`.

## Architectural Decisions

- **Decorator pattern**: Fork awareness, ancestry resolution, caching, and store each in separate decorators maintaining single-responsibility
- **Ancestry resolution**: Delegates to `slippy.Client.ResolveSlip()` via thin `SlipResolverAdapter`. Resolution logic (commit ancestry walking, image tag extraction) is maintained in the `slippy` library, not reimplemented locally.
- **GitHub App auth**: Uses `ghinstallation/v2` for JWT-based GitHub App authentication with per-org installation caching (handled by `goLibMyCarrier/github.GraphQLClient`)
- **Composite cursor pagination**: `timestamp|cityHash64(row_data)` prevents data loss when multiple rows share the same nanosecond timestamp
- **LIMIT n+1 peek**: Request one extra row to determine if a next page exists without a separate COUNT query
- **Named parameters in ClickHouse**: `{name:Type}` syntax for dynamic filter injection
- **Commit SHA uniqueness**: Full 40-char hex SHAs are globally unique, safe for cross-repo resolution

## Technical Debt / Known Issues

- CachedSlipReader is passthrough only — actual caching not yet implemented
- E2E tests require Docker/testcontainers (skipped in CI without Docker)

## Next Steps (Not Yet Implemented)

- Implement actual caching logic in CachedSlipReader
- PR review and merge of `feat/log-search` branch
