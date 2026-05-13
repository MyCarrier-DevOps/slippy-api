# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on the **slippy-api** repository.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->

---

## Build & Test

**Always use Makefile targets, NOT raw `go` / `golangci-lint` commands.** The Makefile encodes the canonical lint config, coverage thresholds, and tool versions used by CI. Raw `go test ./...` may pass while `make test` (and CI) fail because of different flags.

```bash
make lint            # golangci-lint w/ repo config (NOT raw `golangci-lint run`)
make test            # full test suite w/ race + coverage (NOT raw `go test ./...`)
make fmt             # formatters w/ install-tools (NOT raw `gofmt -l`)
make tidy            # go mod tidy across modules
make check-sec       # gosec scan
make build           # build all binaries
make clean           # remove build artifacts
make generate-spec   # regenerate OpenAPI spec
make generate-client # regenerate slippy-client from spec
```

Available targets: `grep -E "^[a-z_-]+:" Makefile`.

**Quick verification (acceptable during iteration):**
```bash
go build ./...   # quick compile check
go vet ./...     # quick static analysis
```

But **final gate before commit MUST be `make lint && make test`** — CI compares against Makefile output.

**For subagents:** brief them to use `make lint` / `make test` explicitly. Don't let them substitute raw commands. If a target is unfamiliar, list them first via `grep -E "^[a-z_-]+:" Makefile`.

## Architecture Overview

**slippy-api** is an HTTP API service that exposes read and write operations on Slippy routing slips. It acts as the persistence layer between CI/CD pipeline agents (Slippy CLI) and ClickHouse storage.

```
slippy-api/          — main HTTP service (port 8080)
  internal/
    domain/          — interfaces (SlipReader, SlipWriter) + type aliases from goLibMyCarrier/slippy
    handler/         — Huma v2 HTTP handlers (read: slip_handler, write: slip_write_handler)
    infrastructure/  — adapters: ClickHouse store, Redis cache, SlipWriterAdapter, AncestryAdapter
    config/          — env-based config loading
    middleware/      — auth, tracing
    telemetry/       — OTel setup
    e2e/             — integration tests (testcontainers Redis)
slippy-client/       — generated OpenAPI Go client (oapi-codegen)
```

Key design: the `SlipWriterAdapter` wraps `slippy.Client` from `goLibMyCarrier/slippy`. After step mutations, it calls `hydrateAndPersist` for non-aggregate pipeline steps to flush computed `*_status` columns to ClickHouse. Aggregate steps skip this because the library handles their `Load + Update` path internally.

## Slippy Library Dependency

The core logic lives in `github.com/MyCarrier-DevOps/goLibMyCarrier/slippy`.

- **State machine reference:** `.github/STATE_MACHINE_V3.md` in [goLibMyCarrier](https://github.com/MyCarrier-DevOps/goLibMyCarrier)
- **Invariant tests:** `TestClient_AggregateBuildFailurePropagatesSlipFailed`, `TestClient_PromoteSlip_Immutable`, `TestClient_AbandonSlip_Immutable` in goLibMyCarrier/slippy

### Slippy Bump Checklist

When bumping `goLibMyCarrier/slippy` to a new version:

1. `cd slippy-api && go get github.com/MyCarrier-DevOps/goLibMyCarrier/slippy@vX.Y.Z`
2. `go mod tidy`
3. Check if `slippy.SlipStore` interface gained new methods — update `mockSlipStore` in `internal/infrastructure/store_test.go` to implement them.
4. Check `go build ./...` — fix any signature mismatches.
5. Run `go test ./... -short` — fix any test assumptions broken by behavioral changes.
6. Run `make lint` — 0 issues expected.
7. Scan for `PromoteSlip`/`AbandonSlip` call sites followed by step mutations — since v1.3.77, slip.status is preserved after those terminal operations (no longer overwritten by late step events).
8. No source code changes expected beyond `go.mod`, `go.sum`, and test mocks.

### Behavioral Notes (v1.3.77+)

- `checkPipelineCompletion` short-circuits on `Completed`, `Abandoned`, `Promoted` (was `Completed` only before v1.3.77). Post-`PromoteSlip`/`AbandonSlip` terminal step events no longer overwrite `slip.status`.
- `UpdateStepWithStatus` now calls `checkPipelineCompletion` for terminal pipeline-level step events. This means `store.Load` is called from within the library even for aggregate steps — the adapter's `hydrateAndPersist` (which calls `Update`) is still correctly skipped, but `Load` itself is not.
- `slippy.SlipStore` gained `UpdateSlipStatus(ctx, correlationID, status)` — an atomic INSERT SELECT that avoids a full `Load + Update` round-trip when updating only `slip.status`.

## Conventions & Patterns

- Domain interfaces (`SlipReader`, `SlipWriter`) are defined in `internal/domain/` and backed by infrastructure adapters.
- Type aliases in `domain/slip.go` keep handlers decoupled from direct `goLibMyCarrier/slippy` imports.
- Hydration (`hydrateAndPersist`) is non-fatal — step events are durable in `slip_component_states` even if the post-write hydration fails.
- Mock implementations of `slippy.SlipStore` live in `internal/infrastructure/store_test.go`. The compile-time check `var _ slippy.SlipStore = (*mockSlipStore)(nil)` in `z_slipstore_interface_test.go` will catch interface drift on every build.

### Critical Design Pattern: Read-Your-Own-Writes Overlay

**Problem:** With ClickHouse `async_insert=1`, a row inserted by a library call (e.g. `CompleteStep`) may not be visible to the immediately following `SELECT` inside `Load()`. If `hydrateAndPersist` calls `Load` and then `Update`, it can write a stale `running` status back to `routing_slips`, permanently violating I5 (materialization consistency invariant).

**Fix pattern (`overlayPipelineStep` in `slip_writer.go`):** Capture `writtenAt := time.Now()` before the library INSERT call. After `Load()` returns in `hydrateAndPersist`, overlay the just-written `(stepName, status, writtenAt)` into `slip.Steps[stepName]` before `Update()`. The overlay wins if `CompletedAt == nil || writtenAt.After(*CompletedAt)`.

**When to use:** Any new code path that calls `hydrateAndPersist` (or any equivalent Load+Update pattern) immediately after a library INSERT must apply this overlay. The event log row is the truth; the overlay makes the in-memory slip consistent with it before the write-back.

**Aggregate steps:** Do NOT apply this pattern for aggregate steps in slippy-api — those are handled inside goLibMyCarrier by `overlayComponentState` (PR #59). The `isPipelineStep()` guard ensures `hydrateAndPersist` is never called for aggregate or component steps.

**Reference:** goLibMyCarrier PR #59 (`overlayComponentState` in `slippy/clickhouse_store.go`) is the sibling fix for aggregate steps. `STATE_MACHINE_V3.md` §I5 documents the invariant.
