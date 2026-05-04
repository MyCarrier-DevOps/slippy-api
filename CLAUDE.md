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

```bash
# Build all modules
cd slippy-api && go build ./...
cd slippy-client && go build ./...

# Run tests (unit only, skip integration/e2e containers)
cd slippy-api && go test ./... -short -timeout 60s

# Full test suite (requires Docker for testcontainers)
cd slippy-api && go test ./...

# Lint (installs golangci-lint if missing)
make lint
```

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
