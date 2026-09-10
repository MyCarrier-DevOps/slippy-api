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
   `threshold-total` live so local and CI never drift). On non-main branches it
   finishes with mutation testing (`make mutation`, `mutest -diff origin/main`) —
   surviving mutants mean missing assertions; add tests rather than skip.
4. **The commit is gated.** In checkouts armed by `/go-repo-init`, a pre-commit
   hook re-runs fmt, lint, test, and (on non-main branches, when the run would
   judge exactly what the commit stages) mutation before any `git commit`, and
   blocks the commit until they pass. If the hook reports it skipped mutation,
   that is not a pass — deal with the reason it names. Do not try to bypass
   the gate — fix the failure it reports.

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
3. Check if `slippy.SlipStore` interface gained new methods — update `mockSlipStore` in `internal/infrastructure/store_test.go` to implement them. Also `asyncInsertSlipStore` in `internal/e2e/dedup_lock_e2e_test.go`; `z_slipstore_interface_test.go` asserts the interface at compile time so `go vet` names every gap. **v1.3.100 added `Repave`** (DEVOPS-231) and changed `CreateSlipForPush`'s same-commit lookup from `LoadLiveByCommit` to `LoadByCommit` so ended rows are visible for repave — a double whose `LoadByCommit` has no nil-safe default panics.
4. Check `go build ./...` — fix any signature mismatches.
5. Run `go test ./... -short` — fix any test assumptions broken by behavioral changes.
6. Run `make lint` — 0 issues expected.
7. Scan for `PromoteSlip`/`AbandonSlip` call sites followed by step mutations — since v1.3.77, slip.status is preserved after those terminal operations (no longer overwritten by late step events).
8. Check whether `slippy.DispatchIntent` gained a value — if so, widen `enum:"something,nothing"`
   on `CreateSlipInput.Body.Dispatch` in `internal/handler/slip_write_handler.go` — or whether the
   guard/seeding mechanics documented in the comment on that field changed (`emptyRunGuardApplies`
   ordering, the `failed` carve-out, component seeding in `initializeSlipForPush`). Nothing in
   this module tests those mechanics and the library functions are unexported, so this step is the
   only detector; a missed enum widening fails closed in pushhookparser (422 → DLQ) while the
   local suite stays green.
9. Otherwise no source code changes expected beyond `go.mod`, `go.sum`, and test mocks.

**v1.3.102 adds Postgres migration v5 (`one_slip_per_commit`, DEVOPS-231 Phase B).** No
interface change and no source change here — but deploying this bump is what APPLIES that
migration, because `slippy-migrator`'s default `target-version` is latest. v5 adds
`uq_routing_slips_repo_sha` on `(lower(repository), commit_sha)` plus `ON DELETE CASCADE` FKs
from `slip_component_states` / `slip_ancestry` on `correlation_id`. Two things follow:

- **Do not deploy it to an environment whose cleanup has not run.** v5 refuses a database that
  still holds more than one `routing_slips` row per commit, or any orphan child row, and the
  migrator's per-migration transaction rolls it back — the pre-deploy Job then crash-loops with
  the recorded version stuck at 4, and because ensurers only run after all migrations succeed,
  new step columns and indexes do not land either. `target-version` cannot cap the up-path.
  Recovery is to run the cleanup script (attached to DEVOPS-231) or pin back to a pre-v5 goLib.
  Both dev and prod `ci` were cleaned on 2026-09-09, so this bump is safe for them.
- **Do not pre-create the index or the FKs by hand.** v5 is idempotent by name but asserts by
  shape: it RAISEs unless the object it kept has exactly the expected definition. A same-named
  object of another shape — a `NO ACTION` or `NOT VALID` FK, an index without `lower()`, an
  invalid leftover from a hand-run `CREATE INDEX CONCURRENTLY` — fails the migration by design.

Once v5 is applied, `Create` can return `ErrDuplicateSlip` for a real concurrent same-commit
insert, which arms `handleDuplicateSlipBackstop` in the library for the first time. slippy-api
needs no change for that: it surfaces the library's result as it already does.

### Behavioral Notes (v1.3.77+)

- `checkPipelineCompletion` short-circuits on `Completed`, `Abandoned`, `Promoted` (was `Completed` only before v1.3.77). Post-`PromoteSlip`/`AbandonSlip` terminal step events no longer overwrite `slip.status`.
- `UpdateStepWithStatus` calls `checkPipelineCompletion` for terminal pipeline-level step events, so `store.Load` is invoked from within the library. Under Postgres that is a normal atomic read; slippy-api no longer wraps writes in a post-write `Load + Update` (the ClickHouse-era adapter path was removed).
- `slippy.SlipStore` gained `UpdateSlipStatus(ctx, correlationID, status)` — an atomic INSERT SELECT that avoids a full `Load + Update` round-trip when updating only `slip.status`.

## Conventions & Patterns

- Domain interfaces (`SlipReader`, `SlipWriter`) are defined in `internal/domain/` and backed by infrastructure adapters.
- Type aliases in `domain/slip.go` keep handlers decoupled from direct `goLibMyCarrier/slippy` imports.
- Step writes are atomic under Postgres — the library persists the status column, component state, and history in one transaction — so slippy-api performs no post-write hydration/overlay (removed with the ClickHouse backend).
- Mock implementations of `slippy.SlipStore` live in `internal/infrastructure/store_test.go`. The compile-time check `var _ slippy.SlipStore = (*mockSlipStore)(nil)` in `z_slipstore_interface_test.go` will catch interface drift on every build.
- ClickHouse test mocks come from upstream `goLibMyCarrier/clickhouse/clickhousetest` (since the v1.3.100 bump, DEVOPS-343). They were vendored in `internal/testsupport/clickhousetest` while upstream's `MockConn` lacked `InsertFormat`/`QueryFormat` against clickhouse-go/v2 v2.48.0+; goLibMyCarrier#82 fixed that upstream and the vendored copy was deleted.

### The claim endpoint, and why its two writes are ordered (DEVOPS-285)

`POST /slips/{correlationID}/claim` exists for callers that adopt a correlation ID they did
not create — today only pushhookparser's rerunner, which reuses the slip returned by a
commit lookup and then dispatches workflows against it. An ended slip is repave-eligible, so
a same-commit push in the window before the adopter's first step write deletes the row and
every later write from that run 404s. `in_progress` is not in `repaveableSlipStatusesSQL`, so
claiming closes the window: the push dedups onto the adopter's slip instead.

- **Do not reorder the two writes.** The library has no atomic status-plus-history primitive
  at slip level, so the marker append and the status update are separate transactions.
  Marker first means a failed status write leaves the slip at its prior, still-repaveable
  status with the attempt recorded — nothing dispatched, next push recovers the commit.
  Status first would leave a slip `in_progress` with nothing running and no record of why,
  and because `in_progress` is not repaveable, every later same-commit push would dedup onto
  a slip that never reports again. Three adapter tests assert the order. One caveat to "with
  the attempt recorded": a repave that lands *between* the two writes deletes the row and the
  just-written marker with it, and the status write then 404s — on that path the surviving
  guarantee is the 404, not the audit trail. (A repave *before* the claim fails the `Load` and
  writes nothing.)
- **A repeat claim on an `in_progress` slip is a deliberate no-op, never a 409.** The writes
  run on a cancellation-detached context, so a caller that times out can see an error against
  a slip that is already claimed; pushhookparser recovers by retrying the whole message and
  claiming again. Any deterministic rejection (409 on already-claimed, or a refusal keyed on
  the prior status) would burn that retry budget and DLQ the rerun. The no-op de-duplicates
  sequential retries only — it grants no exclusivity, and the contract does not promise any.
  `TestSlipWriterAdapter_ClaimSlip_RepeatClaimIsANoOp` pins it.
- **Claiming a `promoted` slip overwrites the primary promotion record with no restoration
  path.** `UpdateSlipStatus` has no transition guard. `completed` self-heals (step columns are
  untouched, so the completion check writes `completed` back); `promoted` cannot, because a
  feature-branch slip never has `prod_steady_state` completed. The residual record is the
  lagging, descendant-keyed `slip_ancestry.parent_status`. This is not refused — the rerunner
  adopts whatever the commit lookup returns and a refusal would DLQ it — so the prior status
  in the marker message is the only in-slip trace: do not reword `claimMarker`'s format
  casually. DEVOPS-202 (persist `promoted_to`) is the prerequisite for a non-destructive claim.
- **`claimMarkerStep` should stay off the configured pipeline steps — and only the running
  service can check that.** It is `slip_claimed` so no phase-duration reader backfills a real
  step's `StartedAt` from the marker. The live pipeline config is a Vault document loaded at
  runtime (`SLIPPY_PIPELINE_CONFIG`); the JSON configs shipped with the library are examples, so
  no unit test here can prove the invariant. `main.go` warns at boot via
  `infrastructure.ClaimMarkerStepCollision`; the unit test only proves absence from the synthetic
  test config. `push_parsed` belongs to the library's own in-place reset marker; an adoption is
  not a push.
- **No new `SlipStatus` value, ever, for this.** DEVOPS-282 records why: an older reader
  hitting an unknown value falls through `IsTerminal`'s `default: return false`, so
  `IsLive()` reads it as live.

**Deploy the API before any client that calls a new route or sends a new field.** huma emits
`additionalProperties: false`, so an unknown body property is answered with 422 — a client
that leads the API fails hard rather than degrading. This applies to `/claim` and equally to
new request fields such as `dispatch` (DEVOPS-341). `TestClaimSlip_RejectsBadInput` pins the
behaviour so the constraint lives in the tests rather than in folklore.

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
