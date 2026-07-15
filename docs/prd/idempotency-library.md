# PRD — Generic Idempotency Library for Spring Boot REST services

> Source: synthesized from the design conversation. Grounded in `CONTEXT.md`
> (glossary + settled decisions), `docs/adr/0001` (pluggable store), and
> `docs/adr/0002` (concurrency model). Terms are used as defined in the glossary.

## Problem Statement

Teams building REST services on Spring Boot repeatedly hit the same class of bug:
the *same* operation runs more than once. A client retries after a network
timeout, a user double-clicks a submit button, or an at-least-once message
delivery redelivers — and a payment is charged twice, an order is created twice,
stock is decremented twice. Each team reinvents an ad-hoc guard (a unique
constraint here, a "check before insert" there), inconsistently and usually
without handling the hard parts: concurrent duplicates in flight, replaying the
*original* response, distinguishing a genuine retry from a key reused for a
different payload, and behaving sanely when the backing store is down.

Developers want a single, generic, drop-in mechanism they can opt into per
endpoint, without coupling it to each endpoint's payload schema, and without
having to reason about the concurrency and failure edge cases themselves.

## Solution

A reusable Spring Boot library that provides **Idempotency** as an opt-in,
per-endpoint concern via an `@Idempotent` annotation. A protected endpoint
computes a single **effective key** per request (from a client-supplied header or
from a body field), and the library stores the operation's response indexed by
that key. A repeat with the same effective key returns the stored result instead
of re-executing the effect.

The developer chooses, per endpoint:
- **How the key is resolved** — **header strategy** (client-supplied, e.g.
  `X-Idempotency-Key`) or **body-field strategy** (a JSONPath into the request
  body, e.g. `$.order.id`).
- **Which `IdempotencyStore` backs it** — `redis` (best-effort, fast) or
  `postgres` (exactly-once when the effect writes to that same database).
- **Policy overrides** — TTL, key-required, store-failure posture
  (fail-open/closed), and concurrency behavior (reject vs wait).

Everything else is a sensible global default configurable in
`application.properties`, so the common case is a single-line annotation. The
library handles collision detection, in-flight concurrency, response replay, and
store-failure posture uniformly, with one client-facing status taxonomy across
both stores.

## User Stories

1. As a Spring Boot developer, I want to mark an endpoint idempotent with a single
   `@Idempotent` annotation, so that I add duplicate protection without writing
   plumbing.
2. As a developer, I want to choose the **header strategy** so that a client that
   reuses one `X-Idempotency-Key` across retries and double-clicks is protected.
3. As a developer, I want to choose the **body-field strategy** with a JSONPath
   (e.g. `$.order.id`) so that clients I don't control are deduplicated by business
   identity even when they send no coordinated key.
4. As a developer, I want the library to produce exactly **one effective key** per
   request, so that behavior is unambiguous and never runs two stacked checks.
5. As a developer, I want the effective key scoped by **endpoint + authenticated
   principal + key value**, so that one client's key can never read another
   client's cached response and the same value on different routes never collides.
6. As an API client, I want a retried request with the same key to return the
   **original response** (status, headers, body), so that my retry is safe.
7. As an API client, I want a **replayed response flagged** with
   `Idempotency-Replayed: true`, so that I (and my logs) can tell a replay from a
   fresh execution.
8. As a developer, I want a request that reuses a key with a **different payload**
   to be rejected with **422**, so that a client bug never silently returns a stale
   result for a different operation.
9. As a developer, I want two **concurrent** requests with the same key to be
   handled safely, so that a double-click processed in parallel still executes the
   effect only once.
10. As a developer, I want the default concurrency behavior to **reject** the
    in-flight duplicate with **409** immediately, so that a duplicate storm never
    exhausts my servlet thread pool.
11. As a developer, I want to opt an endpoint into **wait** mode
    (`whenInProgress=WAIT`), so that the second caller can receive the real result
    instead of a retry signal, when I accept the thread cost.
12. As an API client, I want a uniform **409 + `Retry-After`** for any transient
    "come back and resend the same key" situation, so that my retry logic is
    identical regardless of endpoint mode.
13. As an API client, I want a **terminal 409 `response_unavailable`** (no
    `Retry-After`) when the effect completed but its response can't be replayed, so
    that I understand the operation happened even though I can't fetch its body.
14. As a developer, I want only **2xx** responses cached and any error to **release
    the key**, so that a transient 500 doesn't freeze into a permanent failure for
    every retry.
15. As a developer, I want an **expired key to behave as a new key**, so that after
    the TTL window a legitimate reuse simply re-executes without special "expired"
    errors.
16. As a developer, I want a **default 24h TTL** that I can override per endpoint,
    so that the retry window matches the operation.
17. As an operator, I want to configure global defaults (store, TTL, failure
    posture, concurrency) in `application.properties`, so that I tune behavior per
    environment without touching code.
18. As a developer, I want per-endpoint annotation attributes to **override** the
    global defaults, so that `/payments` and `/newsletter` can differ while sharing
    one library.
19. As an operator, I want **fail-open** by default when the store is unavailable,
    so that a Redis blip doesn't take my whole API down.
20. As a developer, I want to mark critical endpoints **fail-closed** (503 when the
    store is unavailable), so that a payment never executes without idempotency
    protection.
21. As a developer, I want the key to be **required by default** (400 if the header
    or body field is absent), so that I don't silently ship an unprotected endpoint
    I thought was protected.
22. As a developer, I want to relax that with `keyRequired=false`, so that
    idempotency can be a client opt-in on endpoints where that's acceptable.
23. As a developer, I want the key treated as an **opaque string** (size/charset
    bounded) rather than a validated ULID, so that I'm not coupled to the client's
    id scheme.
24. As a developer, I want to pick the `postgres` store for endpoints whose effect
    is a write to that database, so that I get **exactly-once** on the effect.
25. As a developer, I want to pick the `redis` store for endpoints whose effect is
    external or non-transactional, so that I get fast best-effort protection without
    a false promise of atomicity.
26. As a developer, I want to add only the store module(s) I use, so that my
    application doesn't drag in Redis or JDBC dependencies it doesn't need.
27. As a developer, I want to select a store by **qualifier string**
    (`store="postgres"`), so that I can also point at a custom or multi-datasource
    store bean.
28. As a developer, I want the application to **fail fast at startup** if an
    `@Idempotent` is misconfigured (both/neither of header and fieldPath, an
    invalid TTL, or a store qualifier with no bean on the classpath), so that I
    catch mistakes at boot, not on the first request.
29. As an operator, I want **metrics** for replays, 422 collisions, 409
    concurrency, and fail-open activations, so that I can observe idempotency in
    production.
30. As a security-conscious developer, I want volatile response headers filtered on
    replay (denylist) and **`Set-Cookie` always blocked**, so that a replay never
    re-emits another request's session or a stale token.
31. As a developer, I want oversized responses (over `max-body-size`) to be handled
    without re-executing the effect, so that large bodies don't blow up the store
    and a retry doesn't double-run the operation.
32. As a developer with a Redis-clustered deployment, I want each record to be a
    single key, so that idempotency works under Redis Cluster with no cross-slot
    operations.
33. As a developer, I want the library to buffer the request body so both the
    body-field strategy and my controller's `@RequestBody` can read it, so that key
    extraction doesn't consume the input stream.
34. As a maintainer of the library, I want a store-agnostic `IdempotencyStore`
    contract, so that a future store (e.g. MySQL) is a new module with no change to
    `core`.
35. As a developer using `postgres` + `WAIT`, I want a crashed primary's waiter to
    safely take over and execute, so that a mid-flight crash still results in the
    operation running exactly once.

## Implementation Decisions

**Modules (build units).** Three modules so consumers pull only what they use
(ADR 0001):
- `idempotency-core` — annotation, web integration, policy engine, key resolution,
  fingerprint, response capture/replay, config, startup validation, metrics, and
  the `IdempotencyStore` SPI. No store dependency.
- `idempotency-store-redis` — `RedisIdempotencyStore`, auto-configured under
  qualifier `"redis"`.
- `idempotency-store-postgres` — `PostgresIdempotencyStore`, auto-configured under
  qualifier `"postgres"`, plus a versioned schema migration.

**Deep modules and their interfaces (no file paths; interfaces described by role):**

- **`IdempotencyStore` (SPI)** — the central abstraction. Operations: reserve the
  effective key with a fingerprint and TTLs (returns *reserved* or the existing
  record), find a record, complete a record with the captured response, release a
  record, and an overridable `await` for the wait path. Two implementations with
  **different guarantees** (ADR 0001). The qualifier constant lives in each store
  module.
- **`IdempotencyEngine`** — orchestrates the flow over an `IdempotencyStore` and
  resolved policy: resolve effective key → compute fingerprint → reserve →
  branch on key lifecycle (in-progress / completed / collision) → execute or replay
  → complete on 2xx / release on error → apply store-failure posture. Encodes the
  `CONTEXT.md` decisions and the ADR 0002 status taxonomy. Framework-free; depends
  only on abstractions of request/response, store, and policy.
- **`KeyResolutionStrategy`** — `resolve(request) → raw key`. `HeaderKeyStrategy`
  reads the configured header; `BodyFieldKeyStrategy` applies a JSONPath over the
  **buffered raw JSON** (ADR-aligned: HTTP-level, faithful replay, decoupled from
  the controller's deserialized type).
- **`Fingerprint`** — pure function of method + path + normalized (canonical
  key-ordered) body → SHA-256. Used for the 422 collision check.
- **`EffectiveKeyFactory` / scope** — composes method + path + principal + raw key
  into the canonical scoped identity. Principal falls back to a sentinel on
  unauthenticated routes; endpoints are isolated.
- **`PolicyResolver`** — resolves effective per-request policy from `@Idempotent`
  attributes plus global defaults, honoring the inheritance sentinels (`""` for
  string attributes, `DEFAULT` enum members). `keyRequired` is a plain boolean with
  a fixed default of `true` (does not inherit global).
- **`CapturedResponse` + `ResponseReplayer`** — capture status, denylist-filtered
  headers, and body (subject to `max-body-size`); replay them verbatim with
  `Idempotency-Replayed: true`.

**Web integration (thin adapter).** A Servlet `Filter` wraps request/response for
buffered body reads and response capture; a `HandlerInterceptor` reads the
`@Idempotent` on the resolved handler and drives the `IdempotencyEngine`. The
engine holds the logic; the adapter holds framework coupling.

**Annotation contract (`@Idempotent`).** Exactly one of `header` / `fieldPath`
(validated at startup). Inheritable attributes with sentinels: `store` (qualifier,
`""` inherits), `ttl` (Duration string, `""` inherits, placeholder-capable),
`onStoreFailure` (`OPEN | CLOSED | DEFAULT`), `whenInProgress`
(`REJECT | WAIT | DEFAULT`). Non-inheritable: `keyRequired` (boolean, default
`true`). Idiomatic with `@Transactional`/`@Cacheable`/`@Autowired`.

**Status taxonomy (ADR 0002), uniform across stores.** `2xx` result/replay;
`409 + Retry-After` for retryable (reject / wait-timeout / key gone); `409` without
`Retry-After` (`response_unavailable`) for completed-but-not-replayable (terminal);
`422` fingerprint collision (terminal); `400` missing required key; `503`
fail-closed with no store.

**Concurrency (ADR 0002).** Default `REJECT` (immediate 409). `WAIT` polls the key
state (~100ms + jitter) in `core` for stores without native blocking (Redis).
`PostgresIdempotencyStore` realizes reject/wait via native locking on the
reservation `INSERT` (`lock_timeout`), and **safely self-promotes** the waiter on a
primary rollback — an accepted store divergence.

**`RedisIdempotencyStore` data model.** One Redis **Hash** per record, key
`{prefix}{sha256(method \x00 path \x00 principal \x00 keyValue)}` (hashed for
collision-safety and bounded memory; readable scope kept as hash fields). Reserve
and complete are single Lua scripts (atomic). Lifecycle via the key's TTL: reserve
→ lock-ttl (~30s); complete → response-ttl (24h); failure → `DEL`. Single-key ops
→ Redis Cluster friendly.

**`PostgresIdempotencyStore` schema.** Table `idempotency_record` with a composite
primary key `(http_method, path, principal, idempotency_key)` (`principal NOT NULL
DEFAULT ''` to avoid `NULL != NULL`). Columns: `fingerprint` (SHA-256),
`response_status` (nullable — null marks completed-but-not-yet-written),
`response_headers` (JSONB, denylist-filtered), `response_body` (BYTEA),
`created_at`, `expires_at` (indexed for the cleanup job). Reservation is `INSERT
... ON CONFLICT DO NOTHING` **inside the effect's transaction**; the response is a
later best-effort `UPDATE`. A scheduled job deletes expired rows.

Reservation SQL shape (decision-encoding, not full impl):

```sql
INSERT INTO idempotency_record (http_method, path, principal, idempotency_key, fingerprint, expires_at)
VALUES (:m, :p, :u, :k, :fp, now() + :ttl)
ON CONFLICT (http_method, path, principal, idempotency_key) DO NOTHING;
-- 1 row  -> reserved; execute effect in this transaction; commit atomically
-- 0 rows -> existing committed row: fingerprint mismatch -> 422;
--           response_status NULL -> 409 response_unavailable; else replay
```

**Global configuration.** `application.properties` under `idempotency.*`: the four
inheritable defaults (`default-store`, `default-ttl`, `default-on-store-failure`,
`default-when-in-progress`), concurrency internals (`lock-ttl`, `wait-timeout`),
key policy (`key.max-length`; charset is fixed, not configurable), scope
(`scope.principal-enabled`, `scope.principal-claim`), replay (`replay.header-name`,
`replay.header-denylist` with `Set-Cookie` always forced), `response.max-body-size`,
`metrics.enabled`, and per-store namespaces (`store.redis.*`, `store.postgres.*`).
There is no `key-required` global default by design.

## Testing Decisions

**What makes a good test here:** assert *external behavior* — the HTTP outcome
(status, `Idempotency-Replayed`, `Retry-After`), whether the effect ran once or
twice, and what a repeat returns — never internal call sequences or private state.
Prefer real backing services (Testcontainers) over mocks for the stores so the
tests exercise the actual reservation/locking semantics.

**Modules under test (all of them):**

- **`IdempotencyStore` contract test** — one abstract behavioral suite defining the
  store contract (reserve returns reserved-or-existing; completed record replays;
  release frees the key; fingerprint retained; TTL/lock lifecycle; wait/await
  semantics; the accepted Postgres self-promotion divergence). Run against
  `RedisIdempotencyStore` (Testcontainers Redis), `PostgresIdempotencyStore`
  (Testcontainers Postgres), and an in-memory fake. Highest-value suite:
  guarantees both stores honor one contract.
- **`IdempotencyEngine` decision-tree tests** — behavioral tests over the in-memory
  fake store, covering: fresh execution + caching; replay with flag; 422 collision;
  409 reject in-progress; wait-mode outcomes; error releases key; expired = new;
  fail-open vs fail-closed; `response_unavailable`. No web or DB.
- **Pure unit tests** — `Fingerprint` (normalization, order-independence, mismatch),
  `KeyResolutionStrategy` (header present/absent; JSONPath hit/miss over raw JSON),
  `PolicyResolver` (annotation overrides, sentinel inheritance, `keyRequired`
  default).
- **End-to-end (MockMvc + Testcontainers)** — a real `@Idempotent` controller over
  HTTP, exercising header and body-field strategies against both stores: duplicate
  suppression, concurrent double-submit, collision 422, 400 on missing key, replay
  headers, and the fail-open/closed postures.

**Prior art:** greenfield — no existing tests to mirror. This PRD establishes the
conventions: JUnit 5, Testcontainers for Redis/Postgres, MockMvc for the web layer,
and an abstract contract-test base class parameterized per store implementation.

## Out of Scope

- **Business-identity deduplication as a durable invariant.** The library provides
  idempotency (a cache/reservation with a TTL), not a permanent uniqueness
  guarantee; a true business invariant still belongs in a database unique
  constraint. There is no separate "deduplication" mechanism (glossary decision).
- **Exactly-once for external side effects.** When the effect is an external call
  (payment gateway, email, another service), neither store provides atomicity;
  callers propagate their key to the provider's own idempotency (ADR 0001).
- **Additional store implementations** (MySQL, Oracle, etc.). Each is a future
  module; the shared SQL base is extracted only when a second concrete SQL store
  exists (ADR 0001).
- **Push-based wait detection** (Redis Pub/Sub / `BLPOP`) and **waiter
  self-promotion in the polling model** — both are noted v2 evolutions in ADR 0002.
- **Client-side SDK / retry helper.** The library is server-side; generating and
  reusing keys is the client's responsibility.
- **A default header name that the annotation can omit** — the presence-based
  strategy selection makes `header` always explicit (ADR-aligned consequence).

## Further Notes

- The design is fully captured in `CONTEXT.md`, `docs/adr/0001`, and
  `docs/adr/0002`; those are the source of truth and this PRD must stay consistent
  with them. Notably, the two stores deliberately differ in concurrency mechanics
  (polling vs native locking) and in one behavioral edge (Postgres safely
  self-promotes a waiter on primary failure; Redis returns 409).
- Suggested build order (vertical slices): (1) `core` engine + `IdempotencyStore`
  SPI + in-memory fake + header strategy, proven end-to-end with the fake; (2)
  `store-redis`; (3) `store-postgres` with the transactional model and migration;
  (4) body-field strategy; (5) metrics, startup validation, and configuration
  polish.
- Publishing note: no issue tracker was configured in this workspace, so this PRD
  was written to the repository. To turn it into tracked, agent-ready issues later,
  set up the tracker and run the issues breakdown against this document.
