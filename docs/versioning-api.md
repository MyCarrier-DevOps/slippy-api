# API Versioning Guide

This document describes the versioning strategy for the Slippy API and provides guidance for introducing future versions.

## Versioning Model

- **Major versions** are expressed in the URL path: `/v1/`, `/v2/`, etc.
- **Minor/patch versions** are tracked in the OpenAPI spec's `info.version` field (e.g. `1.2.0`), not in the URL.
- A new URL version is created **only** for breaking changes.
- Non-breaking changes (new optional fields, new endpoints, new optional query params) are added to the current version and the spec version is bumped (e.g. `1.0.0` -> `1.1.0`).

## Current Layout

Legacy (unversioned) endpoints are preserved for backward compatibility. All versioned routes live under `/v1/`:

| Legacy | Versioned |
|---|---|
| `GET /health` | `GET /v1/health` |
| `GET /slips/{correlationID}` | `GET /v1/slips/{correlationID}` |
| `GET /slips/by-commit/{owner}/{repo}/{commitSHA}` | `GET /v1/slips/by-commit/{owner}/{repo}/{commitSHA}` |
| `POST /slips/find-by-commits` | `POST /v1/slips/find-by-commits` |
| `POST /slips/find-all-by-commits` | `POST /v1/slips/find-all-by-commits` |
| `GET /slips/{correlationID}/image-tags` | `GET /v1/slips/{correlationID}/image-tags` |
| `GET /logs/{correlationID}` | `GET /v1/logs/{correlationID}` |

Both sets of routes use the same handlers. The legacy routes exist solely for backward compatibility and should be considered deprecated for new integrations.

## Breaking vs Non-Breaking Changes

### Breaking Changes (require a new major version)

- Removing or renaming a response field
- Changing a field's type (e.g. `string` -> `int`)
- Removing an endpoint
- Making a previously optional request field required
- Changing error response codes for existing conditions
- Changing authentication requirements

### Non-Breaking Changes (stay in current version)

- Adding a new optional field to a response
- Adding a new endpoint
- Adding a new optional query parameter
- Adding a new optional request body field
- Returning additional enum values (if consumers are tolerant)

### Security-Corrective Changes (stay in current version)

Closing a security hole is **not** deferred to a new major version, even when the change matches an entry in the breaking list above — most obviously "Changing authentication requirements", and "Removing an endpoint" when the endpoint is the exposure.

Read without this carve-out, the breaking-change list forbids its own remedy: a route that is unauthenticated by mistake could only be secured in `/v2`, which leaves the hole open in `/v1` for as long as `/v1` is supported. A deprecation alias is no help either, since the added authentication breaks pre-existing callers whether or not the old path still resolves.

What a security-corrective change requires instead:

- Call it out explicitly in the PR description and the release notes, naming the routes affected and the new requirement.
- State the caller-visible effect. "Callers with no credential now receive `401`" is the useful sentence, not "auth was tightened".
- Identify the affected consumers where the repository allows it, and say so plainly when it does not — ingress and network policy live in ManagementInfra, so who was calling an endpoint usually cannot be settled here.
- Regenerate the spec and client if the route is part of the published contract, so consumers see the requirement.

Recorded instances:

- **DEVOPS-217** — API-key auth changed from opt-in per operation to fail-closed, and `GET /v1/admin/schema-version` (unauthenticated) was removed in favour of `GET /v1/diagnostics/clickhouse-schema-version` (read key required). Both the retirement and the added authentication land in `/v1` under this carve-out.
- **DEVOPS-217 follow-up** — `maxItems: 1000` added to `commits` on `POST /v1/slips/find-by-commits` and `/find-all-by-commits`, so a request above that now returns `422` where it previously returned `200`. That matches "Changing error response codes for existing conditions" in the breaking list, and lands in `/v1` under this carve-out: the field was the input to an unbounded per-commit GitHub ancestry walk, and one 1 MiB request measured 262,133 outbound calls — more than the App's hourly budget, which fails ancestry resolution platform-wide.

  Caller-visible effect: a request carrying more than 1000 commits is rejected with `422`; at or below 1000 nothing changes. The only production consumer is `slippy-find`, which sends `1 + (parents x depth)` — 51 at its default depth of 25, 101 at the `--depth 50` in its own help text — so the limit is roughly 10x the documented worst case. The generated `slippy-client` is byte-identical after regeneration, because oapi-codegen emits no validation for `maxItems`; only the published spec gains the constraint.

## How to Add a New Major Version

The API uses [Huma v2](https://huma.rocks/) groups for versioning. Adding a new version follows this pattern:

### 1. Create a new route group

The current v1 setup uses a **fan-out group** to register both legacy (unversioned) and `/v1/` routes simultaneously:

```go
// Current setup — fan-out registers every route at both "" and "/v1"
grp := huma.NewGroup(api, "", "/v1")
```

When introducing v2, switch to **separate single-prefix groups** for each version. The legacy unversioned routes can be dropped or kept as a third group:

```go
// v1 routes
v1 := huma.NewGroup(api, "/v1")
handler.RegisterHealthRoutes(v1)
handler.RegisterRoutes(v1, slipHandlerV1)
handler.RegisterImageTagRoutes(v1, ith)
handler.RegisterCIJobLogRoutes(v1, clh)

// v2 routes (only changed endpoints)
v2 := huma.NewGroup(api, "/v2")
handler.RegisterRoutesV2(v2, slipHandlerV2)
```

**Update the public-route allowlist in the same change.** Dropping the fan-out group changes which routes exist, and `publicRoutes` in `internal/middleware/auth.go` is keyed on `"METHOD /path"`. Keeping the unversioned group means keeping `GET /health`; dropping it means dropping that entry. Adding `/v2/health` means adding `GET /v2/health`, or the new probe returns `401` to kubelet on every replica.

`TestBuildHandler_EveryOperationIsSecuredOrAllowlisted` checks both directions and will fail the build on a missing entry *and* on one left stale, so this cannot ship silently — but fix it deliberately rather than reacting to a red test.

### 2. Create new input/output structs for changed endpoints

Version-specific request/response types live in the handler layer:

```go
type GetSlipOutputV2 struct {
    Body struct {
        Slip      *domain.Slip `json:"slip"`
        Metadata  *SlipMeta    `json:"metadata"` // new in v2
    }
}
```

### 3. Domain interfaces stay unchanged

Versioning is a handler-layer concern. The domain interfaces (`SlipReader`, `ImageTagReader`, etc.) should not change for versioning purposes. The v2 handler adapts between the new contract shape and the domain layer.

### 4. Generate the new contract

```bash
make generate-spec
```

This produces `api/v1/openapi.json`. For v2, update the generate target to also produce `api/v2/openapi.json`.

## Mixed Versions vs Full Version Sets

When introducing a new version, there are two valid strategies for how consumers interact with the API.

### Strategy 1: Mixed Versions (recommended for internal APIs)

Consumers call `/v1/` for unchanged endpoints and `/v2/` only for endpoints that changed.

**When to use:** Internal APIs, microservice-to-microservice communication, teams that control both the producer and consumer.

**Best practices:**

- Document clearly which endpoints are available at which version. The OpenAPI spec does this automatically -- v1 and v2 routes are differentiated by tags.
- When an endpoint moves to v2, keep it on v1 as well (marked deprecated) during a migration window.
- Consumers should set a per-endpoint base URL rather than a single global base URL, so they can adopt v2 changes incrementally.
- Avoid mixing more than 2 active versions. If v3 arrives, deprecate and sunset v1.

**Example consumer usage:**

```
# Unchanged endpoint -- still on v1
GET /v1/health

# Changed endpoint -- use v2
GET /v2/slips/{correlationID}

# Unchanged endpoint -- still on v1
GET /v1/slips/{correlationID}/image-tags
```

### Strategy 2: Full Version Set (recommended for public/external APIs)

Every endpoint is registered on the new version, even if unchanged. Consumers commit fully to one version by switching their base URL.

**When to use:** Public APIs, third-party integrations, APIs where consumers cannot easily mix versions.

**Best practices:**

- Register all endpoints on the new version, even if unchanged. Consumers switch their base URL once (`/v1/` -> `/v2/`).
- Use the same handler functions for unchanged endpoints -- no code duplication.
- Mark the old version as deprecated in the OpenAPI spec (`deprecated: true` on operations).
- Announce a sunset date for the old version and communicate it in API documentation.

**Example consumer usage:**

```
# Consumer fully on v2 -- all calls use /v2/
GET /v2/health
GET /v2/slips/{correlationID}
GET /v2/slips/{correlationID}/image-tags
```

### General Best Practices (either strategy)

- Never remove a version without a deprecation period (minimum 1 release cycle).
- Add `Sunset` and `Deprecation` response headers to deprecated version endpoints.
- Keep contracts (`api/v1/openapi.json`, `api/v2/openapi.json`) committed and CI-validated.
- Communicate version lifecycle in API documentation and changelogs.

## Deprecation Process

1. **Mark deprecated:** Set `deprecated: true` on operations in the OpenAPI spec via Huma's `Operation` struct.
2. **Add headers:** Use middleware on the deprecated version group to add `Sunset: <date>` and `Deprecation: true` response headers.
3. **Log usage:** Log warnings when deprecated endpoints are called, so you can track which consumers still depend on them.
4. **Sunset:** After the announced sunset date, return `410 Gone` or remove the routes entirely.

## Contract Management

Each major version has its own contract file:

```
slippy-api/
  api/
    v1/
      openapi.json    # Generated, committed to repo
    v2/
      openapi.json    # Generated when v2 is introduced
```

Generate contracts with:

```bash
make generate-spec
```

The committed spec should always match the running API. Consider adding a CI step that regenerates the spec and fails if it differs from the committed version (spec drift detection).
