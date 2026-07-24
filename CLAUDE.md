# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on the **slippy-api** repository.

This repo's Go workflow — the idiomatic-Go conventions, the RED-test-first delivery loop, the security preflight, and the coverage gate — is owned by the **go-devkit** plugin (an apm dependency declared in [apm.yml](apm.yml); its machinery is installed outside the repo tree under `apm_modules/` via `apm install`). The go-devkit block below is the authoritative description of that workflow and is kept in sync by `/go-repo-init` — do not edit it by hand. The sections after it record the project-specific facts the plugin cannot know: this repo's module layout, coverage policy, and house rules.

<!-- BEGIN go-devkit -->
## Development workflow (go-devkit)

This repository uses the **go-devkit** Claude Code plugin. The idiomatic Go
conventions this project enforces (naming, error handling, package layout,
concurrency, HTTP clients, testing, security) live in the plugin and are loaded
by the `go-tdd` skill — read them before writing Go code.

For features and bugfixes, use the **`go-tdd`** skill — it drives the full loop:

1. **RED test first (code changes only).** Write a failing table-driven test
   before the implementation, then make it pass, then refactor — confirm it
   fails for the intended reason first. This does **not** apply to meta changes
   (renaming the app / `APPLICATION`, config, docs, dependency bumps); make those
   directly and verify with `/go-verify`.
2. **Preflight before coding.** Run `/go-preflight` (`make check-sec`) after
   planning and before implementing. If `govulncheck` flags a Go standard-library
   CVE, upgrade the toolchain (`brew upgrade go`, or `mise use -g go@latest`)
   before continuing; if it flags a dependency, `make bump`.
3. **Verify after.** Run `/go-verify` when the task is done — it runs `make fmt`,
   `make lint`, `make test`, and the plugin's coverage gate (which reads the CI
   `threshold-total` live so local and CI never drift).

**Pin the Go version to a full patch release, and keep it in sync.** The `go`
directive in the module's `go.mod` (e.g. `go 1.26.5`, not `go 1.26`) and the
builder image in the Dockerfile (`golang:1.26.5`) must name the same patch. CI
intentionally floats on the patch level (`go-version: "1.26"`); bump it by hand
for a new minor or major.
<!-- END go-devkit -->

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

**Coverage gate (per module): `make test` must satisfy the CI total-coverage threshold (currently 80%).** New code needs tests before you commit — CI fails the build below the gate. If a function is genuinely not unit-testable (`main()`, a thin `realDeps()` wiring closure, a `*pgxpool.Pool` call that needs a live DB), don't lower the threshold: keep the module above it by covering the rest, and make the untestable code thin — extract the real logic behind a small interface so it can be exercised with a fake (see `advisoryLock`/`pgExecer` in `slippy-migrator/run.go`).

**For subagents:** brief them to use `make lint` / `make test` explicitly. Don't let them substitute raw commands. If a target is unfamiliar, list them first via `grep -E "^[a-z_-]+:" Makefile`.

## Architecture Overview

**slippy-api** is an HTTP API service that exposes read and write operations on Slippy routing slips. It is the persistence layer between CI/CD pipeline agents (Slippy CLI) and **Postgres** (the operational slip store); ClickHouse is retained only for the non-slip readers and federated reporting.

```
slippy-api/          — main HTTP service (port 8080)
  internal/
    domain/          — interfaces (SlipReader, SlipWriter) + type aliases from goLibMyCarrier/slippy
    handler/         — Huma v2 HTTP handlers (read: slip_handler, write: slip_write_handler)
    infrastructure/  — adapters: Postgres slip store, Redis cache, SlipWriterAdapter, AncestryAdapter
    config/          — env-based config loading
    middleware/      — auth, tracing
    telemetry/       — OTel setup
    e2e/             — integration tests (testcontainers Redis)
slippy-client/       — generated OpenAPI Go client (oapi-codegen)
```

Key design: the `SlipWriterAdapter` wraps `slippy.Client` from `goLibMyCarrier/slippy`, whose `SlipStore` is now the `PostgresStore` (pgx, direct). Step writes are **atomic** — the library writes the `*_status` column in the same transaction as the component-state upsert and history append — so slippy-api performs no post-write read-modify-write. The ClickHouse-era `hydrateAndPersist`/overlay machinery was removed in the Postgres migration (DEVOPS-127).

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
- `UpdateStepWithStatus` calls `checkPipelineCompletion` for terminal pipeline-level step events, so `store.Load` is invoked from within the library. Under Postgres that is a normal atomic read; slippy-api no longer wraps writes in a post-write `Load + Update` (the ClickHouse-era adapter path was removed).
- `slippy.SlipStore` gained `UpdateSlipStatus(ctx, correlationID, status)` — an atomic INSERT SELECT that avoids a full `Load + Update` round-trip when updating only `slip.status`.

## Conventions & Patterns

- Domain interfaces (`SlipReader`, `SlipWriter`) are defined in `internal/domain/` and backed by infrastructure adapters.
- Type aliases in `domain/slip.go` keep handlers decoupled from direct `goLibMyCarrier/slippy` imports.
- Step writes are atomic under Postgres — the library persists the status column, component state, and history in one transaction — so slippy-api performs no post-write hydration/overlay (removed with the ClickHouse backend).
- Mock implementations of `slippy.SlipStore` live in `internal/infrastructure/store_test.go`. The compile-time check `var _ slippy.SlipStore = (*mockSlipStore)(nil)` in `z_slipstore_interface_test.go` will catch interface drift on every build.

### Removed: Read-Your-Own-Writes Overlay (ClickHouse-era)

The `overlayPipelineStep` / `hydrateAndPersist` read-your-own-writes overlay was **removed** in
the Postgres migration (DEVOPS-127). It existed only to work around ClickHouse `async_insert`
visibility: a row inserted by a library call might not be visible to the immediately following
`SELECT`, so a `Load + Update` could write back a stale status and violate I5 (materialization
consistency).

Under Postgres this cannot happen — a committed write is immediately visible (MVCC) and the
library writes the status column atomically in the same transaction as the component-state
upsert. **Do NOT reintroduce a post-INSERT `Load + Update` (or any equivalent overlay) in
slippy-api.** The sibling aggregate-step fix (`overlayComponentState` in goLibMyCarrier) is
likewise unnecessary for the Postgres store. `STATE_MACHINE_V3.md` §I5 still documents the
invariant itself; only the ClickHouse-specific workaround is gone.
