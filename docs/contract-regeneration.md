# Contract & Client Regeneration Guide

This document describes the step-by-step process for regenerating API contracts and the Go client after making changes to the Slippy API.

## When to Regenerate

Regenerate whenever you change something that affects the API surface:

- Add, remove, or rename an endpoint
- Change request/response struct fields (add, remove, rename, change type)
- Change path parameters, query parameters, or request body shape
- Change authentication requirements on an endpoint
- Change the API version number in `main.go`
- Add a new API version group (e.g. `/v2/`)

You do **not** need to regenerate for:

- Internal logic changes (handler business logic, infrastructure, domain layer)
- Middleware changes that don't affect the API contract
- Configuration or environment variable changes
- Test-only changes

## Prerequisites

Install the code generation tool (one-time setup):

```bash
make install-oapi-codegen
```

This installs `oapi-codegen` v2.6.0. Verify with:

```bash
oapi-codegen -version
```

## Step-by-Step Regeneration

### Step 1: Make your API changes

Edit the relevant handler files in `slippy-api/internal/handler/`. Changes that affect the contract include:

- **Input/output structs** -- These define the request/response schemas. Located at the top of each handler file.
- **Route registration** -- `huma.Operation` structs define the path, method, operation ID, and security. Located in `Register*` functions.
- **Domain types** -- If a domain type used in a response changes (e.g. `domain.Slip`), the contract changes too.

Example: adding an optional field to the slip response would mean updating the `Slip` struct in the upstream `slippy` library or in `domain/slip.go`.

### Step 2: Run tests to verify your changes work

```bash
make test
```

Ensure all existing tests pass and add tests for any new behavior.

### Step 3: Regenerate the OpenAPI specs

```bash
make generate-spec
```

This runs `TestGenerateOpenAPISpec` which:
1. Boots the API with stub readers
2. Fetches the OpenAPI spec from `/openapi.json`
3. Writes the full spec to `slippy-api/api/v1/openapi.json`
4. Produces a **v1-only, OpenAPI 3.0.3 compatible** spec at `slippy-api/api/v1/openapi-v1.json` (used as input for client generation)

### Step 4: Regenerate the Go client

```bash
make generate-client
```

This command:
1. Runs `make generate-spec` (ensures specs are up to date)
2. Installs `oapi-codegen` if not present
3. Runs `oapi-codegen` with the config at `slippy-client/oapi-codegen.yaml`
4. Overwrites `slippy-client/client.gen.go` with the new types and client methods
5. Runs `go mod tidy` on the client module

### Step 5: Review the generated diff

```bash
git diff slippy-api/api/v1/
git diff slippy-client/client.gen.go
```

Check that:
- New endpoints appear in the spec and client
- Removed endpoints are gone
- Changed fields are reflected in the generated types
- No unexpected changes crept in

### Step 6: Format and lint

```bash
make fmt
make lint
```

### Step 7: Run all tests again

```bash
make test
```

### Step 8: Commit the regenerated files

Commit the following files together with your API changes:

- `slippy-api/api/v1/openapi.json` -- Full OpenAPI spec
- `slippy-api/api/v1/openapi-v1.json` -- v1-only 3.0 spec (codegen input)
- `slippy-client/client.gen.go` -- Generated Go client
- `slippy-client/go.mod` / `go.sum` -- If dependencies changed

## Quick Reference (single command)

For the common case where you just want to regenerate everything after a change:

```bash
make generate-client && make fmt && make lint && make test
```

`generate-client` already calls `generate-spec`, so this single pipeline handles the full cycle.

## CI Automation

You don't strictly need to regenerate locally -- CI handles it as a safety net.

When a PR merges to `main`, the **`regenerate-client`** CI job:

1. Runs `make generate-client` (spec + client)
2. Compares the output to what's committed
3. If there's a diff, commits the updated files back to main with `chore: regenerate API contracts and Go client [skip ci]`
4. If the client changed, the **`release-client`** job tags and releases a new version

**However, best practice is to regenerate locally and include the files in your PR.** This way:
- Reviewers can see the contract diff in the PR
- CI won't need to auto-commit after merge
- You catch issues before merging, not after

## Troubleshooting

### oapi-codegen fails with "unsupported OpenAPI version"

The v1-only spec at `openapi-v1.json` should be OpenAPI 3.0.3. If this error occurs, the `buildV1OnlySpec` function in `main_test.go` may not be downconverting correctly. Check that `"openapi"` is set to `"3.0.3"` in the generated spec.

### Generated client has unexpected type changes

The spec generation uses stub readers (`newStubSlipReader`) which return minimal data. The schema is derived from the Go struct tags, not from actual data. If a type looks wrong, check the struct definition in the handler or domain layer.

### New endpoint missing from generated client

Ensure the endpoint is:
1. Registered on the `grp` variable (not directly on `api`) in `main.go`
2. Tagged with the v1 group (happens automatically via `huma.NewGroup`)
3. Has a `/v1/` prefixed path in `openapi.json`

## File Reference

| File | Purpose |
|---|---|
| `slippy-api/api/v1/openapi.json` | Full OpenAPI 3.1 spec (both legacy + v1 routes) |
| `slippy-api/api/v1/openapi-v1.json` | v1-only OpenAPI 3.0.3 spec (input for oapi-codegen) |
| `slippy-client/oapi-codegen.yaml` | Code generation config (package name, output, what to generate) |
| `slippy-client/client.gen.go` | Generated Go client -- **do not edit manually** |
| `slippy-api/main_test.go` | Contains `TestGenerateOpenAPISpec` and `buildV1OnlySpec` |
