# Context — Idempotency for REST services (Spring Boot)

Domain glossary for this library. Language only — no implementation details.

## Terms

### Idempotency
The mechanism: store the response of an operation indexed by a **key** and, when
the same key repeats, return the stored result instead of re-executing the effect.
There is **a single** mechanism — the key's origin changes neither the mechanism
nor the policy.

### Idempotency Key
A value that identifies **one intent to perform an operation**. Two requests with
the same effective key refer to the same logical operation.

### Effective Key
The **single** key the library uses per request, after the endpoint's strategy is
applied. A request always produces one, and only one, effective key.

### Key Resolution Strategy
Where the effective key is obtained from. Chosen **per endpoint**. Two available:

- **Header strategy** — the key comes from an HTTP header supplied by the client
  (e.g., `X-Idempotency-Key`, ULID). Represents the *client's* intent. Covers
  network retries and double-clicks from a client that **reuses** the same key.

- **Body-field strategy** — the key is extracted from a field of the request body,
  pointed at by a JSONPath on the `@Idempotent` annotation itself (e.g.,
  `@Idempotent(fieldPath = "$.order.id")`). Represents **business identity**.
  Covers distinct requests, without a coordinated key, that refer to the same
  entity (clients we don't control).

### IdempotencyStore
The abstraction that persists the reservation and the cached response of a key.
Pluggable, with different guarantees per implementation (see `docs/adr/0001`).
**Each endpoint chooses** the store: `Redis` (best-effort, fast) or `Postgres`
(exactly-once when the effect writes to the database itself).

### Key lifecycle
An effective key goes through two observable states:
- **Reserved / in-progress** — the first request registered the key atomically and
  is still processing the effect. No response stored yet.
- **Completed** — the response has been stored; repeats return the cache.

A concurrent request that finds the key *in-progress* gets **409** immediately
(default behavior); *waiting* for completion is optional/configurable. If the
process dies before completing, the reservation is released automatically so it
does not **poison** the key.

Both strategies feed the **same** mechanism, under the **same** policy. They are
not stacked checks: each request uses one strategy and produces one effective key.

## Settled decisions
- Generic, reusable library; each endpoint *opts in* and chooses the strategy.
  Clients can be any UI or API clients.
- Always **one** effective key per request.
- Key scope: **endpoint + authenticated principal + key value**. Routes without
  auth fall back to endpoint + value. Endpoints are **isolated**: the same entity
  on different routes does not collide.
- **Single policy** of idempotency for both key origins (same rules for collision,
  expiration, and scope). No separate term/mechanism for "deduplication".
- **Collision** (same key, different body): reject with **422**, comparing a
  *fingerprint* of method + path + normalized body.
- **Caching by status**: only **2xx**. Any non-2xx response, or the handler
  throwing, **releases the key** for a genuine retry.
- **Expiration**: an expired key = a new key (re-executes). No tombstone, no
  "expiration error". Default TTL of **24h**, configurable per endpoint.
- **Store failure (store unavailable)**: **fail-open** by default (let it through
  unprotected); **fail-closed** (503) optional per endpoint. Global default
  adjustable via `application.properties`. Precedence: endpoint > global.
- **Key required** by default: missing header or missing body-path → **400**.
  Configurable with `keyRequired = false` (client opt-in).
- **Key format**: **opaque** string with a configurable size limit and a fixed
  (not configurable) charset, both rejected with **400** outside bounds. ULID is
  a recommended convention for the client, **not** validated/required by the
  server beyond that charset.
- **Replayed response**: signaled with header `Idempotency-Replayed: true` +
  metrics (replay, 422 collision, 409 concurrency, fail-open). Headers reproduced
  via a **denylist** of volatile ones; `Set-Cookie` is **always** blocked.
- **Unavailable response**: when the effect completed but the response can't be
  replayed (not yet written after commit, or body over `max-body-size`), the key is
  kept **completed without a replayable body** and a retry gets **409** with reason
  `response_unavailable`. The effect never re-executes. Same contract in both stores.
