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

### 2026-05-11 — Async-Insert Race Fix in hydrateAndPersist (PR: fix/hydrate-and-persist-async-insert-race)

**Context:** Production slip `b058127d-fe0a-497d-81e6-08edc7ea71b2` (MC.Shipment, `dev_tests` step) showed `dev_tests_status=running` in `routing_slips` despite TestEngine.Worker having called `CompleteStep`. Root cause: same ClickHouse `async_insert=1` visibility race as goLibMyCarrier PR #59, but in the `hydrateAndPersist` code path in `SlipWriterAdapter` rather than inside the library.

**Race mechanism:**
1. CLI/TestEngine calls `POST /v1/slips/{id}/steps/dev_tests/complete`
2. `SlipWriterAdapter.CompleteStep` calls `client.CompleteStep` → writes row to `slip_component_states` (`appendHistoryWithOverrides` in goLibMyCarrier)
3. `hydrateAndPersist` calls `client.Load(correlationID)` → `hydrateSlip` SELECT from `slip_component_states` — but the row just written is not yet visible (async buffer not flushed, ~200ms lag)
4. `Update()` writes the stale `running` status back to `routing_slips.<dev_tests_status>` — permanently stuck (I5 violation)

**Scope:** Affects all pure pipeline steps (`unit_tests`, `secret_scan`, `dev_tests`, `dev_deploy`, `preprod_deploy`, `preprod_tests`, etc.) — any step where `isPipelineStep()` returns `true`. Aggregate steps (`builds`) are already fixed by goLibMyCarrier PR #59 (`overlayComponentState`).

#### Fix (`slippy-api/internal/infrastructure/slip_writer.go`)

1. **`overlayPipelineStep`** — new pure function. Read-your-own-writes safety net: after `Load()` returns inside `hydrateAndPersist`, merges the just-written step state directly into the in-memory `slip.Steps[stepName]` before `Update()`. Mirrors the sentinel path of goLibMyCarrier's `overlayComponentState` (PR #59), applied to `slippy.Step` rather than `ComponentStepData`.
2. **`hydrateAndPersist` signature extended** — now accepts `(stepName string, status StepStatus, writtenAt time.Time)`. `writtenAt` is captured before the library call so the overlay timestamp is always at-or-before the ClickHouse INSERT clock.
3. **Callers updated** — `StartStep` (→ `running`), `CompleteStep` (→ `completed`), `FailStep` (→ `failed`), `SkipStep` (→ `skipped`) each capture `writtenAt := time.Now()` before the underlying library call and pass the appropriate status to `hydrateAndPersist`.
4. **Overlay rule** — `step.CompletedAt == nil || writtenAt.After(*step.CompletedAt)` — the new status wins unless the `Load`-returned value is provably newer. Defensive guard prevents overwriting a concurrent terminal event.

#### Tests

- `TestHydrateAndPersist_AsyncInsertRace_CompleteStep` — core regression: Load returns stale `running`, after overlay persisted step must be `completed`. Mirrors `TestOverlayUpdatesStepsStatus_T1Regression` in goLibMyCarrier.
- `TestHydrateAndPersist_AsyncInsertRace_FailStep` — same for `failed` path.
- `TestHydrateAndPersist_OverlaySkipsNewerStatus` — guard: overlay does not overwrite a newer `CompletedAt` from Load.
- `TestOverlayPipelineStep_NilSlip`, `TestOverlayPipelineStep_MissingStep`, `TestOverlayPipelineStep_NilCompletedAt`, `TestOverlayPipelineStep_OlderCompletedAt` — unit tests for `overlayPipelineStep` helper.

#### Round-1 review findings applied (F1–F5)

Round-1 DA review caught a critical secondary bug in the initial fix and produced five findings:

- **F1 (P1, critical)** — `overlayPipelineStep` guard `step.CompletedAt == nil || writtenAt.After(*step.CompletedAt)` allowed a non-terminal `running` overlay to clobber a terminal `completed` status. Concrete bug: slip `b058127d` had a second `StartStep` at 17:01 arriving after `CompleteStep` at 16:58 — `writtenAt` (`time.Now()`) was after the 16:58 `CompletedAt`, so the overlay wrote `running` over `completed`. **Fix:** added early-return guard `if !status.IsTerminal() && step.CompletedAt != nil { return }` before the timestamp check. This enforces terminal-wins invariant (I2/I5).
- **F2** — Added two new regression tests that FAIL without the F1 guard and PASS with it: `TestOverlayPipelineStep_RunningDoesNotClobberTerminal` (unit) and `TestHydrateAndPersist_StartStep_DoesNotClobberTerminalStatus` (end-to-end via `StartStep`).
- **F3** — Added doc comment to `overlayPipelineStep` explaining `ApplyStatusTransition` set-once `CompletedAt` behaviour on re-run paths.
- **F4** — Added doc comment noting cross-repo duplication with `goLibMyCarrier`'s `overlayComponentState` and explaining why it is intentional.
- **F5** — Added `slip.Steps == nil` early-return guard for nil-map symmetry with `overlayComponentState`'s `Aggregates` nil-init pattern.

#### Spec alignment

| Invariant | Impact |
|---|---|
| **I5** (cached column == event-log-derived) | **Strengthened** — eliminates permanent stale window for pipeline-level steps. External CH readers retain bounded ~200ms staleness during async flush (unchanged); the fix only eliminates permanent staleness from the write-back path. |
| I1–I4 | Unaffected — enforced by `checkPipelineCompletion` inside goLibMyCarrier, which runs before `hydrateAndPersist`. |

#### Out of scope (follow-ups from PR #59 still apply)

1. Cache invalidation — `cache.go` is passthrough; when Dragonfly cache activates, invalidate at `slip_write_handler.go:364`.
2. Crash recovery — if slippy-api crashes between library INSERT and `hydrateAndPersist` Update, no re-fire mechanism exists. Needs WAL or periodic reconciler.

**Sibling fix:** goLibMyCarrier PR #59 (aggregate steps).
**Coverage:** `overlayPipelineStep` 100% · `hydrateAndPersist` 100% · infrastructure package 95.9% · lint 0 issues.

---

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

---

## I5 race resolution — full architecture (Layers 1+2+3)

ADO #82468 — defense-in-depth fix for the TMS "stuck slip" class. The bug shape
was first observed in production slip `436cc68c` (terminal `unit_tests`
regressed to `running` after a same-microsecond concurrent write, leaving the
slip permanently `in_progress`). This section documents the three-tier
architecture that closes the race.

### Pre-fix bug (slip 436cc68c — production race)

```text
                  Pod A (ci_runner_1)                 Pod B (ci_runner_2)
                  -------------------                 -------------------
   t0             POST /v1/.../complete (unit_tests)
                    │
                    │  SlipWriteHandler.completeStep
                    │    └─ SlipWriterAdapter.CompleteStep
                    │         └─ slippy.Client.CompleteStep
                    │              └─ store.UpdateStep(..., COMPLETED)
                    │                   └─ insertComponentState(...)
                    │                        INSERT wait_for_async_insert=0
                    │                        (queued; NOT yet visible)
   t1                                                  POST /v1/.../start (unit_tests)
                                                          │  SlipWriteHandler.startStep
                                                          │   └─ Adapter.StartStep
                                                          │      └─ Client.StartStep
                                                          │         └─ store.UpdateStep(
                                                          │                ..., RUNNING)
                                                          │            └─ insertComponentState
                                                          │               (queued)
   t2             (Pod A INSERT flushes)
   t3                                                  (Pod B INSERT flushes; same µs)

   POST-flush slip_component_states (event log):
     ts=t2  status=completed  (Pod A)
     ts=t3  status=running    (Pod B)   <-- argMax winner!

   ROUTING TABLE
     routing_slips.unit_tests_status = running   (terminal regression)
     routing_slips.status            = in_progress (slip stuck forever)
```

**Root cause:** ClickHouse async-insert visibility race + no serialization at
ANY layer. Two concurrent writers each read an empty/stale event log, both
INSERT, and `argMax(ts)` deterministically picks the later (non-terminal)
write — flipping `completed → running` in the routing table.

### TIER 1 — per-correlationID Dragonfly lock (`withCorrIDLock`)

Wraps `StartStep`, `CompleteStep`, `FailStep`, `SkipStep`, `PromoteSlip`,
`AbandonSlip` adapters. Closes the `Load → mutate → Update` race for the
**aggregate write-back** path (`s.Update(slip)`), which is NOT covered by the
goLib gate.

```text
                  SlipWriteHandler.<verb>Step
                          │
                          │  validateCorrelationID(corrID)    (§M.1.2 UUID guard)
                          │   └─ 400 BadRequest if malformed
                          │
                          │  SlipWriterAdapter.<verb>Step
                          │   └─ withCorrIDLock(ctx, corrID, fn):
                          │
                          ▼
              ┌──────────────────────────────────────┐
              │  SLIPPY_I5_LOCK_ENABLED ?            │
              │    no → fn() (fail-open, rollback)   │
              │    yes ↓                              │
              │                                       │
              │  Dragonfly: SET sliplock:cid:<corrID> │
              │             NX PX 2000ms              │
              │    acquired=false → return            │
              │                  ErrCorrIDWriteInProgress
              │                  (→ 409 in mapWriteError)
              │    acquired=true  ↓                   │
              │                                       │
              │  defer release (CAS-del via Lua,      │
              │                 ctx.WithTimeout 2s    │
              │                 to survive request    │
              │                 cancel)               │
              │                                       │
              │  fn():                                │
              │    slippy.Client.<verb>Step           │
              │     └─ checkTerminalStatus            │
              │     └─ store.UpdateStepWithHistory    │
              │         └─ enforceTerminalMonotonicity  ← TIER 3 (goLib gate)
              │         └─ insertComponentState       │
              │     └─ aggregate write-back           │
              │         └─ hydrateAndPersist          ← TIER 2 (R1 overlay)
              │             └─ store.Update(slip)     │
              └──────────────────────────────────────┘
```

**Key properties:**
- `TryAcquire` is non-blocking — lock-miss returns 409 immediately (no
  thundering herd, no head-of-line blocking).
- Acquire propagates request `ctx` so client-cancellation aborts cleanly
  (MISS-V2-2).
- Release uses `context.WithTimeout(context.Background(), 2s)` so it survives
  request-ctx cancellation (MISS-V2-3).
- TTL = 2s — provisional, sized ~10× presumed p99. Stage-3 measurement
  (§F.3) MUST verify before production rollout.

### Combined race resolution (Pod A + Pod B walkthrough)

How the three tiers compose to defeat the 436cc68c scenario:

```text
                  Pod A (complete)                    Pod B (start, racing)
                  ----------------                    ---------------------
   t0   POST /complete unit_tests
        validateCorrelationID(corrID) → OK
        withCorrIDLock acquire
          SET sliplock:cid:<corrID> NX PX 2000  → OK (acquired)
        ─────────────────  TIER 1 HOLDS  ─────────────────
        Client.CompleteStep
          ↓
          checkTerminalStatus → not terminal (proceed)
          store.UpdateStepWithHistory(..., COMPLETED)
            ↓ TIER 3: enforceTerminalMonotonicity
            ↓   prior = empty → ALLOW
            ↓ insertComponentState (queued)
          ↓
          aggregate write-back
            ↓ TIER 2: hydrateAndPersist
            ↓   reload event log + recompute aggregates
            ↓   store.Update(slip) under lock
        withCorrIDLock release (CAS-del Lua)
        ←────────────  TIER 1 RELEASED  ────────────
        200 OK
   t1                                            POST /start unit_tests
                                                 validateCorrelationID → OK
                                                 withCorrIDLock acquire
                                                   SET sliplock:cid:<corrID>
                                                       NX PX 2000  → OK
                                                   (Pod A already released)
                                                 Client.StartStep
                                                   ↓
                                                   checkTerminalStatus →
                                                     slip.unit_tests = completed
                                                     (TERMINAL)
                                                   ↓
                                                   short-circuit
                                                   → return nil (idempotent skip,
                                                                 per slippy-api
                                                                 v1.3.77+)

                                                 OR if checkTerminalStatus skipped:
                                                   store.UpdateStep(..., RUNNING)
                                                     ↓ TIER 3: gate
                                                     ↓   prior = completed
                                                     ↓   isRecoveryAllowed → false
                                                     ↓   return ErrTerminalAlreadyExists
                                                   → mapWriteError → 409 Conflict
```

**Outcome:** the terminal `completed` write WINS. Pod B is rejected at one of
three layers (terminal-status guard, gate, or — if gate-disabled — at the R1
overlay event-log validator). `argMax` regression is impossible because the
INSERT never happens.

### Defense-in-depth tier stack

```text
                  HTTP boundary    │   slippy-api    │   slippy lib (goLib)
                  ─────────────────┼─────────────────┼─────────────────────
                                    │                  │
   Layer 0       validateCorrelat… │                  │
                  (UUID format)     │                  │
                                    │                  │
   TIER 1        withCorrIDLock    │                  │
                  (Dragonfly per-   │                  │
                   corrID lock,     │                  │
                   2s TTL, fail-open│                  │
                   on nil locker)   │                  │
                                    │                  │
   checkTerminal …                  │  Client.<verb>  │
   Status guard                     │  (slip-level    │
                                    │   short-circuit) │
                                    │                  │
   TIER 2        hydrateAndPersist  │                  │
   (R1 overlay)  (event-log         │                  │
                  recompute +       │                  │
                  validate before   │                  │
                  store.Update)     │                  │
                                    │                  │
   TIER 3        enforceTerminal…   │                  │  clickhouse_store.go
   (goLib gate)  Monotonicity       │                  │  :612 UpdateStep
                  (81-cell allow-   │                  │  :673 UpdateStepWith…
                   list matrix at   │                  │
                   INSERT boundary, │                  │
                   fail-open on     │                  │
                   query err)       │                  │
                                    │                  │
                  ▼                  ▼                  ▼
                  Even if TIER 1 (lock) misses (nil locker, flag off,
                  Dragonfly outage), TIER 2 (R1 overlay) recomputes from
                  the event log and rejects stale writes. Even if TIER 2
                  is bypassed, TIER 3 refuses terminal-regressing INSERTs
                  at the lowest layer.
```

Each tier is independently rollback-able via env flag and fails-open on
transport errors, so a single failed tier degrades to the next without
blocking writes.

### Rollback flag matrix

```text
   GATE flag        | LOCK flag          | Behavior                | Risk profile
   (goLib)          | (slippy-api)        |                          |
   -----------------+--------------------+-------------------------+-----------------
   unset / false    | unset / false      | Pre-cutover (current     | I5 bug live;
                                          production)               status quo
                                                                    |
   unset / false    | true                | Lock only — closes       | Aggregate path
                                          aggregate write-back      | safe; INSERT
                                          race; gate inactive       | path still racy
                                                                    |
   true             | unset / false      | Gate only — refuses      | INSERT path safe;
                                          terminal regressions at   | aggregate path
                                          INSERT; lock inactive     | still racy
                                          (CAN'T serialize same-µs  |
                                          concurrent INSERTs;       |
                                          weaker invariant per      |
                                          §B.8 #17b)                |
                                                                    |
   true             | true                | FULL FIX — both layers   | I5 closed;
                                          active; defense-in-depth  | target state
```

**Rollout sequence (per §G.1):**
1. Both flags default OFF; merge both PRs.
2. Enable `SLIPPY_I5_GATE_ENABLED=true` in staging; soak 48h.
3. Enable `SLIPPY_I5_LOCK_ENABLED=true` in staging; soak 48h.
4. Stage-3 measurement gate (§F.3): verify lock-hold p99 ≤ 500ms.
5. PR 3 (Slippy CLI 409 retry-with-jitter) merged BEFORE step 6.
6. Production GATE on, then LOCK on.
7. Either flag can be flipped OFF for instant rollback.

## Related

- ADO #82468 — TMS infrastructure I5 stuck-slip class
- slippy-api PR #39 (this PR) — Layer 1 (validation) + TIER 1 (lock) + TIER 2
  (R1 overlay) + wires TIER 3 from the library
- goLibMyCarrier PR #72 — TIER 3 INSERT-time monotonicity gate
- Plan v3: `standup-notes/2026/06/resolve-i5-option1-stage2-plan-v3.md`
- Production case: slip `436cc68c` (RCA in `standup-notes/2026/06/`)
- Stage-6 iteration notes: `standup-notes/2026/06/resolve-i5-option1-stage6b-slippy-api.md`
