# Project State — Slippy Application

> **Last Updated:** 2026-03-10
> **Status:** Fork-aware commit lookups implemented

## Overview

Read-only API for CI/CD routing slips. Provides endpoints to query routing slips by correlation ID, commit SHA, and repository. Backed by ClickHouse with optional Dragonfly/Redis caching and OpenTelemetry instrumentation.

## Implemented Systems

### Core API (internal/handler)
- `GET /slips/{correlationID}` — Load slip by correlation ID
- `GET /slips/by-commit/{owner}/{repo}/{commitSHA}` — Load slip by repo + commit
- `POST /slips/find-by-commits` — Find first matching slip from commit list
- `POST /slips/find-all-by-commits` — Find all matching slips from commit list
- `GET /health` — Health check
- Image tag resolution via BuildInfoReader

### Infrastructure (internal/infrastructure)
- **SlipStoreAdapter** — Adapts upstream `slippy.SlipStore` to read-only `domain.SlipReader`
- **ForkAwareSlipReader** — Decorator that handles forked repository commit lookups (see Recent Changes)
- **CachedSlipReader** — Dragonfly/Redis caching decorator (passthrough, cache logic planned)
- **BuildInfoReader** — Resolves per-component image tags from ClickHouse ci.buildinfo

### Middleware
- API key authentication (`middleware/auth.go`)

### Telemetry
- OpenTelemetry tracing and metrics (`telemetry/telemetry.go`)

### Configuration
- Environment variable-based config (`config/config.go`)

## Recent Changes

### 2026-03-10: Fork-Aware Commit Lookups
**Problem:** `/slips/by-commit`, `/slips/find-by-commits`, and `/slips/find-all-by-commits` failed when queried with a forked repository name. The upstream library filters by repository name in ClickHouse, so queries using a fork name (e.g., `fork-user/repo`) couldn't find slips stored under the parent repo name (e.g., `org/repo`).

**Solution:** Added `ForkAwareSlipReader` decorator (`internal/infrastructure/fork_aware.go`) that:
1. Tries the normal lookup first
2. On `ErrSlipNotFound` (or empty results for `FindAllByCommits`), queries ClickHouse directly by `commit_sha` (without repository filter) to resolve the actual stored repository name
3. Retries the lookup with the resolved repository name

**Files changed:**
- `internal/infrastructure/fork_aware.go` — New decorator implementation
- `internal/infrastructure/fork_aware_test.go` — 18 unit tests covering all paths
- `main.go` — Wired `ForkAwareSlipReader` between `SlipStoreAdapter` and `CachedSlipReader`

**Wiring order:** Handler → CachedSlipReader → ForkAwareSlipReader → SlipStoreAdapter → ClickHouseStore

## Current Focus

Fork-aware commit lookups completed and validated.

## Architectural Decisions

- **Decorator pattern for fork awareness**: Placed fork resolution logic in a separate decorator rather than modifying the store adapter, maintaining single-responsibility
- **Fallback-only approach**: Normal lookups are unaffected; the cross-repo resolution only runs when the primary lookup fails with `ErrSlipNotFound`
- **Commit SHA uniqueness**: Git commit SHAs are globally unique (full 40-char hex), so commit-only lookups are safe for resolving the actual repository

## Technical Debt / Known Issues

- Pre-existing `errcheck` lint issues in `buildinfo.go` (unchecked `rows.Close()`)
- Pre-existing `gocritic/unnamedResult` in `cache.go`, `store.go`, `buildinfo.go`
- Pre-existing `unused` function `cacheKey` in `cache.go`
- CachedSlipReader is passthrough only — actual caching not yet implemented

## Next Steps (Not Yet Implemented)

- Implement actual caching logic in CachedSlipReader
- Address pre-existing lint issues
