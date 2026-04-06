# API Versioning & Contract Generation Plan

This document captures the design decisions and implementation approach for adding API versioning and contract generation to the Slippy API.

## Context

The Slippy API originally exposed all endpoints without a version prefix (e.g. `/slips/{id}`, `/health`). We introduced `/v1/` prefixed endpoints while keeping the unversioned endpoints as legacy for backward compatibility. We also generate static API contracts (OpenAPI spec files) for the v1 API.

## What Are API Contracts?

An **API contract** is a formal, machine-readable specification that defines the interface between your API and its consumers. It answers: "What can I call, with what inputs, and what will I get back?"

For this project, the contract is an **OpenAPI 3.1 JSON file** that describes:
- Every endpoint (path, method)
- Request parameters (path params, query params, headers)
- Request/response body schemas (field names, types, required/optional)
- Authentication requirements
- Error response formats

**Why contracts matter:**
- **Consumer confidence** -- clients know exactly what to expect; breaking changes are visible in diffs
- **Code generation** -- generate typed clients (Go, TypeScript, etc.) automatically
- **Contract testing** -- CI can verify the API still matches its published contract
- **Documentation** -- the spec powers `/docs` (Stoplight Elements) and can be imported into Postman, etc.

**What the contract looks like** -- a committed `api/v1/openapi.json` file:
```json
{
  "openapi": "3.1.0",
  "info": { "title": "Slippy API", "version": "1.0.0" },
  "paths": {
    "/v1/slips/{correlationID}": {
      "get": {
        "operationId": "v1-get-slip",
        "parameters": [{ "name": "correlationID", "in": "path" }],
        "responses": {
          "200": { "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Slip" } } } }
        },
        "security": [{ "apiKey": [] }]
      }
    }
  },
  "components": {
    "schemas": {
      "Slip": { "type": "object", "properties": { "correlation_id": { "type": "string" } } }
    }
  }
}
```

Huma v2 generates this at runtime at `/openapi.json`. The static file committed at `slippy-api/api/v1/openapi.json` serves as the versioned contract.

## API Versioning Strategy

**Approach: URL path prefix (`/v1/`) using Huma's built-in `huma.NewGroup`**

| Strategy | Pros | Cons |
|---|---|---|
| **URL prefix** `/v1/...` | Simple, discoverable, cache-friendly, native Huma support | URL changes between versions |
| Header-based (`Accept: vnd.v1+json`) | Clean URLs | Hard to test/debug, no Huma support |
| Query param (`?version=1`) | Easy to add | Non-standard, conflicts with Huma query validation |

**URL prefix wins** -- it's the industry standard, trivially supported by Huma, and the simplest to implement.

### How Huma `NewGroup` works

Huma v2 has `huma.NewGroup(api, prefixes...)` which:
- Returns a `*Group` that implements `huma.API` (drop-in replacement)
- Automatically prefixes all registered route paths
- When multiple prefixes are given, **fans out** each `Register` call into one registration per prefix
- For non-empty prefixes, auto-prefixes OperationIDs (`get-slip` -> `v1-get-slip`) and adds a `v1` tag
- Inherits parent middleware (auth works automatically)

Using `huma.NewGroup(api, "", "/v1")`:
- `""` prefix: routes registered at original paths with original OperationIDs (legacy)
- `"/v1"` prefix: routes registered at `/v1/...` paths with `v1-` prefixed OperationIDs

**Zero handler code changes needed.** All `Register*` functions already accept `huma.API`.

## Implementation Details

### Route group in main.go

```go
grp := huma.NewGroup(api, "", "/v1")
handler.RegisterHealthRoutes(grp)
handler.RegisterRoutes(grp, h)
handler.RegisterImageTagRoutes(grp, ith)
handler.RegisterCIJobLogRoutes(grp, clh)
```

Every endpoint exists at both paths:

| Legacy (unchanged) | v1 (new) |
|---|---|
| `GET /health` | `GET /v1/health` |
| `GET /slips/{correlationID}` | `GET /v1/slips/{correlationID}` |
| `GET /slips/by-commit/{owner}/{repo}/{commitSHA}` | `GET /v1/slips/by-commit/{owner}/{repo}/{commitSHA}` |
| `POST /slips/find-by-commits` | `POST /v1/slips/find-by-commits` |
| `POST /slips/find-all-by-commits` | `POST /v1/slips/find-all-by-commits` |
| `GET /slips/{correlationID}/image-tags` | `GET /v1/slips/{correlationID}/image-tags` |
| `GET /logs/{correlationID}` | `GET /v1/logs/{correlationID}` |

### Contract generation

A test (`TestGenerateOpenAPISpec`) boots the API, fetches `/openapi.json`, and writes it to `slippy-api/api/v1/openapi.json`. Gated behind `GENERATE_SPEC=1` so it doesn't run in normal test suites.

Generate with: `make generate-spec`

### Known behavior: `next_page` URL in logs endpoint

The `buildNextPageURL` function in `ci_job_log_handler.go` hardcodes `/logs/...` as the path. When called via `/v1/logs/...`, the `next_page` response still points to `/logs/...` (without `/v1`). This works because both paths route to the same handler.

## Files Modified

| File | Change |
|---|---|
| `slippy-api/main.go` | Route registration via `huma.NewGroup(api, "", "/v1")` |
| `slippy-api/main_test.go` | v1 endpoint tests + spec generation test |
| `slippy-api/internal/e2e/e2e_test.go` | Same group pattern in test server |
| `makefile` | `generate-spec` target |
| `slippy-api/api/v1/openapi.json` | Generated contract file |
| `VERSIONING.md` | Versioning guide for the team |

No handler files changed. No domain files changed. No infrastructure files changed.

## Related Documentation

- [VERSIONING.md](../VERSIONING.md) -- Developer guide for future versioning, deprecation, and contract management
