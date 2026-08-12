# Auth rate limiting with Fibonacci backoff — design

**Status:** approved, not yet implemented
**Ticket:** DEVOPS-217 follow-up
**Date:** 2026-08-12

## Problem

`slippy-api` is internet-exposed at `https://slippy-api.api.mycarrier.tech` and authenticates
with two static bearer tokens compared by `subtle.ConstantTimeCompare`. Nothing in the
service limits how often a caller may present a wrong credential. An attacker — single host
or distributed — can guess continuously at whatever rate the service will serve.

The previous hardening pass added a 60-character minimum key length. That is a *length*
floor, not an entropy measure: it cannot distinguish a random 60-character key from a
repeated character. Length alone is therefore a single point of failure, and one whose
strength we cannot verify from inside the service. A rate limit holds regardless of how good
the deployed keys actually are, which is the property that makes it worth building.

Secondary benefits: it bounds volumetric abuse of an internet-facing endpoint, and it makes
credential-probing visible in telemetry.

## Threat model

| Attacker | Shape | What stops them |
|---|---|---|
| Single host, fast | Floods guesses from one address | Ladder reaches the 7-day cap in ~31 requests, then every packet resets the clock |
| Single host, patient | Paces guesses to stay under the ladder | ~30 guesses costs ~15.6 days; then 2 guesses per 7-day silence |
| Distributed botnet | Many addresses, few guesses each | Per-IP ladder caps each source; total throughput stays negligible against the keyspace |
| Holder of a leaked key | Authenticates successfully | **Out of scope** — rate limiting does not address this. Rotation does. |

The distributed case is deliberately *not* addressed by a global limit. See "Rejected
alternatives".

## Measured facts

These were established empirically against the test environment, not inferred from
configuration. The distinction matters: the Istio mesh config declares
`gatewayTopology.numTrustedProxies: 2`, and reading that alone would have produced a wrong
design.

Probes sent to the unauthenticated `/health` endpoint from a host with known public IP
`184.97.153.128`, correlated by unique `User-Agent` in the `istio-ingressgateway` access log.

| Probe | `x_forwarded_for` seen at gateway | Outcome |
|---|---|---|
| baseline | `184.97.153.128,172.68.3.180` | real client, then Cloudflare edge |
| `X-Forwarded-For: 1.2.3.4` | `1.2.3.4,184.97.153.128,172.68.35.81` | forged entry survives, prepended |
| `X-Forwarded-For: 1.2.3.4, 5.6.7.8` | `1.2.3.4, 5.6.7.8,184.97.153.128,172.68.35.81` | both forged entries survive |
| `CF-Connecting-IP: 9.9.9.9` | *never reached the cluster* | Cloudflare rejected with 403 at the edge |
| `X-Envoy-External-Address: 8.8.8.8` | `184.97.153.128,172.68.35.81` | passes through, header ignored by gateway |

Conclusions:

1. **Cloudflare appends the true client IP to whatever `X-Forwarded-For` the caller sent.**
   Attacker-controlled entries appear on the **left**. Trusting the leftmost entry means the
   limiter never fires — an attacker rotates the header per request and never accrues a
   penalty.
2. **The real client IP sat second-from-the-right in every probe**, independent of how many
   entries were injected. The rightmost is always the Cloudflare edge address appended by
   the gateway from the actual TCP peer.
3. **`CF-Connecting-IP` cannot be forged through Cloudflare.** The edge 403s the attempt
   before it reaches the cluster.

Topology confirmed while gathering this: `slippy-api` runs **2 replicas**, has **no Istio
sidecar** (the `sidecar.istio.io/inject: true` annotation is inert because the namespace
carries no injection label), and is routed by a `VirtualService` bound to
`istio-system/default`. Two replicas is why counter state must be shared rather than
per-process.

### Confirmed at the pod

The above was measured at the *gateway*. What the pod actually receives was then observed
directly, with a filtered packet capture in an ephemeral debug container on
`slippy-api-test` (allowlisting only forwarding headers, so no `Authorization` value was
ever captured).

Forged request — `X-Forwarded-For: 1.2.3.4, 5.6.7.8` — as received by the application:

```
x-forwarded-for:          1.2.3.4, 5.6.7.8,184.97.153.128,172.68.35.82
cf-connecting-ip:         184.97.153.128
x-envoy-external-address: 172.68.35.82
```

| Header | Value | Usable as identity? |
|---|---|---|
| `CF-Connecting-IP` | real client | **Yes** — unaffected by XFF forgery, and forging it directly is 403'd at the edge |
| `X-Forwarded-For` | real client 2nd-from-right | **Yes**, indexed from the right only |
| `X-Envoy-External-Address` | Cloudflare edge | **No** |

### Finding for the mesh owners

`X-Envoy-External-Address` resolves to the **Cloudflare edge address, not the client**,
which confirms `gatewayTopology.numTrustedProxies: 2` over-skips: there is one proxy
(Cloudflare) in front of the gateway, so the value should be `1`.

Anything relying on Envoy's computed client address today — Istio-level rate limiting, IP
allowlists, `AuthorizationPolicy` source ranges — is keying on the Cloudflare edge rather
than the caller. The edge address is also not stable per client (`172.68.35.82` and
`172.68.3.179` were both observed for the same source within a minute), so it is not usable
as an identity even incidentally.

This is out of scope for this change and should be raised separately. It is also the reason
identity is resolved in the application rather than delegated to Envoy.

## Design

### Client identity

Resolved in order, first match wins:

1. `CF-Connecting-IP` — the only header proven unforgeable through Cloudflare.
2. `X-Forwarded-For`, indexed **from the right** at a configurable offset (default 2),
   never leftmost.
3. `RemoteAddr` — in-cluster callers bypass Cloudflare entirely, and the pod's TCP peer
   cannot be forged.

Stored as a truncated SHA-256, mirroring the existing `keyFingerprint` in `auth.go`, so a
Redis compromise does not yield a list of client addresses.

The offset is configurable (`SLIPPY_XFF_DEPTH`) rather than hardcoded because it is a
property of the deployment topology, and the one value most likely to be wrong. The resolved
IP is logged so it can be verified against real traffic.

**Residual gap:** any header-based identity fails if an attacker reaches the Istio gateway
directly, bypassing Cloudflare. Closing that requires restricting the gateway's load
balancer to Cloudflare IP ranges — infrastructure work, outside this service, recommended
separately.

### The ladder

```
BASE = 10s                 the unit the multiplier scales
F    = 2                   free failures before any delay
CAP  = 604800              7 days, the maximum lockout duration
n                          consecutive failures for this identity

delay(n) = BASE × fib(n - F), iterating until the value exceeds CAP, then CAP
```

The multiplier scales a 10-second base rather than one second. The floor matters far more
than the top of the ladder: a first penalty of 10s versus 1s is the difference between a
meaningful cost and a rounding error, and everything downstream inherits it.

The cap applies to the **resulting duration**, not to the Fibonacci index. This is what
makes the computation safe: the sequence is generated iteratively and the loop exits as soon
as it passes 604,800, so `int64` overflow is structurally impossible rather than clamped
away. `n` itself continues incrementing without limit.

| failure | delay | failure | delay |
|---|---|---|---|
| 1–2 | free | 18 | 4.4h |
| 3 | 10s | 21 | 18.8h |
| 5 | 30s | 24 | 3.3d |
| 8 | 130s | 25 | 5.4d |
| 12 | 14.8m | **26+** | **7d** |
| 15 | 62.8m | | |

Reaching the cap takes 25 failures over ~14 days of perfectly-paced attempts.

**Attempts made while already locked out count.** Each one increments `n` and resets
`lockUntil = now + delay(n)`. Persistence is therefore self-defeating: a flood reaches the
cap in 31 requests — under a second at any real attack rate — and every subsequent packet
restarts the 7-day clock. A locked-out attacker receives no credential checks at all.

Escaping requires **7 consecutive days of complete silence** — at every depth, not only at
the cap, because the record's TTL is a flat 7 days regardless of the current lockout (see
Storage). After that the record ages out and the identity starts fresh with 2 free attempts.

**A successful authentication clears the record**, so a correct key restores service
immediately.

### Storage

Redis (Dragonfly), already wired for the slip cache and the dedup `RedisLocker`. Shared
state is required because the service runs 2 replicas; per-process counters would give an
attacker one budget per pod and reset on every deploy.

```
key:  rl:auth:<sha256(identity)[:12]>
val:  { n, lockUntil }
TTL:  ten times the current lockout, and never less than 1 hour
      (refreshed on every attempt)
```

Two different limits, easy to conflate:

- the **lockout** is how long the caller is refused, and it is *capped* at 7 days;
- the **TTL** is how long `n` is remembered, and it has a *floor* of 1 hour and no ceiling
  beyond what the lockout cap implies (70 days once the lockout reaches 7).

| failures | lockout | record TTL |
|---|---|---|
| 1–2 | none | 1h — the floor, since 10 × 0 = 0 |
| 10 | 340s | 1h — floor still binding |
| 12 | 890s | 2.5h — the 10× rule takes over |
| 21 | 18.8h | 7.8d |
| 26+ | 7d (capped) | 70d |

**The TTL must outlive the lockout by a wide margin, or the ladder cannot climb.** If a
record expired when its own lockout did, a patient attacker would wait out a 10-second
lockout, watch the record vanish, and start again from `n = 0` — collecting free guesses
forever without ever passing the first rung.

Holding the record for 10× the lockout closes that: an attacker who waits only 1× still
finds the record alive, so `n` advances instead of resetting, at every depth. Escaping means
staying silent for **10× the current lockout** — 70 days once the cap is reached.

The 10× rule also scales retention to how much an identity has earned. Noise — a scanner
poking twice, a one-off typo — ages out in minutes rather than squatting for a week, while
persistent attackers accrue progressively longer memory. That is better for Dragonfly than
any flat constant: footprint tracks *sustained* attack traffic, not every address that ever
mistyped a key.

The **1-hour floor** exists for the free-allowance phase, where `lockout` is zero and `10 × 0`
would forget the first two failures instantly — letting an attacker sit at 2 guesses per
expiry forever, never entering the ladder at all. The floor holds that strategy to 2
guesses/hour/identity (~48/day) rather than ~1,700/day.

The TTL is self-managing: refreshed on every attempt, aged out naturally once the caller
stops. No sweeper, no separate decay timer.

Read-check-write is a single Lua script so the increment, the lockout extension and the TTL
refresh are atomic across both replicas.

**Fail-open.** If Dragonfly is unavailable the limiter stops limiting and logs a warning. A
cache outage must not become an API outage, and authentication itself still fails closed —
an unauthenticated caller is rejected whether or not the limiter is working.

### Response contract

On lockout: **`429` immediately**. The server never sleeps to enforce a delay — holding
connections open would hand the attacker a resource-exhaustion primitive against the very
control meant to protect the service.

Headers on `401`, `403` and `429` only:

| Header | Meaning here |
|---|---|
| `X-RateLimit-Limit` | Free failure allowance (2) |
| `X-RateLimit-Remaining` | Failures left before backoff begins |
| `X-RateLimit-Reset` | Unix timestamp when the record ages out |
| `Retry-After` | Seconds remaining on the current lockout (429 only) |

**Not emitted on successful responses.** These describe a *failure* budget, and there is no
request quota on authenticated traffic. `X-RateLimit-Limit: 2` on a healthy `200` would tell
every CI caller the API accepts 2 requests, which is false and actively misleading.

### Placement

In `internal/middleware/auth.go`, inside the existing `authorize` path, so it covers every
operation without per-route wiring. The lockout check runs *before* the credential
comparison — checking first would let an attacker guess freely, since the check is the thing
being rate limited.

Public routes (`GET /health`, `GET /v1/health`) are exempt: they require no credential, so
they cannot fail authentication and never enter the ladder. Kubelet probes are unaffected.

### Telemetry

- `auth.result` already distinguishes `wrong_tier` / `invalid_token` / `missing_token`;
  add `rate_limited`.
- Log and emit a metric on aggregate auth-failure rate. This is **observability only, never
  enforcement** — it makes a distributed attempt visible and pageable without giving an
  attacker a lever to degrade service for anyone else.
- Record ladder depth on the auth span so a climbing identity is traceable.

## Rejected alternatives

**Global failure-rate circuit breaker.** Tightening the free allowance for everyone once
aggregate failures cross a threshold. Rejected: it lets an attacker convert a *failed*
guessing campaign into a *successful* denial of service. Cheap junk traffic pushes the global
counter up, every legitimate caller is tightened, and the CI fleet degrades without a single
key being guessed. The distributed case is instead absorbed by the keyspace, which is exactly
where per-source throttling leaves off.

**Rate limiting authenticated requests per API key.** The read key is fanned out to every
onboarded repository through the Actions templates in `admin/`, so a per-key quota is one
bucket shared by the entire CI fleet. A busy day would trip it with no attacker present.
Only failures are counted, and legitimate callers do not fail.

**In-memory per-replica counters.** Two replicas means two budgets, and every deploy resets
the ladder.

**Capping the Fibonacci index instead of the duration.** Expresses the policy obliquely,
breaks if `F` or the base sequence changes, and requires explicit overflow handling that
capping the duration avoids for free.

**Server-side sleep instead of 429.** Converts the control into a connection-exhaustion
vector.

## Testing

- Table-driven ladder tests: free allowance, each Fibonacci step, saturation at exactly
  604,800, and that the value never exceeds the cap regardless of `n`.
- Attempts during lockout extend rather than drain — assert `lockUntil` moves forward.
- Success clears the record.
- Identity resolution against the **measured** header shapes above, including the forged-XFF
  cases, asserting the attacker-supplied entry is never selected.
- Fail-open: limiter disabled and requests served when Redis errors.
- Public routes never enter the ladder.
- Header presence on 401/403/429 and **absence** on 200.
- Redis behaviour against `miniredis`, consistent with the existing dedup-lock tests.

## Rollout

1. Ship with enforcement disabled, logging the resolved client IP and the ladder depth that
   *would* have applied.
2. Confirm the logged IP matches `CF-Connecting-IP` across real traffic. The header
   behaviour is already measured at the pod (see Confirmed at the pod), so this is
   validating the implementation, not the assumption.
3. Compare observed auth-failure volume against the ladder to confirm no legitimate caller
   would have been throttled.
4. Enable in test, then production.

`config.Load` gains `SLIPPY_RATE_LIMIT_ENABLED` and `SLIPPY_XFF_DEPTH`. Both get the same
validation treatment as the existing variables — no `%w` wrapping that could echo input into
pod logs.

## Operational runbook

A legitimately throttled caller is cleared by deleting its record; waiting is not the
recovery path:

```
redis-cli --scan --pattern 'rl:auth:*'       # locate
redis-cli DEL rl:auth:<hash>                 # clear
```

The identity hash is on every rate-limit log line, so the record can be found from the logs
without reversing anything.
