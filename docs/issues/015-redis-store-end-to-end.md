# Slice 015 — Redis store: end-to-end (happy path, outage, WAIT)

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

A working `RedisIdempotencyStore` (currently a scaffold whose methods all throw
`UnsupportedOperationException`), replacing every stub with a real implementation
per ADR 0001's data model: one Redis **Hash** per record, keyed by
`{prefix}{sha256(method \0 path \0 principal \0 keyValue)}` (hashed for
collision-safety and bounded memory; readable scope kept as hash fields).
Reserve and complete are single Lua scripts (atomic). Lifecycle rides the key's
TTL: reserve → lock-ttl; complete → response-ttl; failure → `DEL`. Single-key
operations only, so it works under Redis Cluster with no cross-slot ops.

Unlike Postgres, Redis has no transactional-participation question — this store
is a standalone best-effort cache, so this slice covers the whole SPI in one
pass: reserve/find/complete/release with fencing (a fenced `complete`/`release`
must no-op via the Lua script checking the stored fence token), plus verifying
this behaves correctly under a real outage and under WAIT mode against a real
container (not just assumed to inherit correctness from the generic engine).

## Acceptance criteria

- [x] `RedisIdempotencyStore.reserve/find/complete/release` are real
      implementations (no `UnsupportedOperationException`), atomic via Lua
      scripts for reserve and complete.
- [x] The Slice 014 contract test suite passes against `RedisIdempotencyStore`
      backed by Testcontainers Redis.
- [x] A `@Idempotent(store="redis")` MockMvc end-to-end test proves replay works
      against a real (Testcontainers) Redis.
- [x] `complete`/`release` are no-ops when the caller's fence token no longer
      matches the current hash's stored token.
- [x] A genuine Redis connection failure (e.g. a stopped/paused container)
      surfaces as `StoreUnavailableException`, exercising the existing
      fail-open/fail-closed posture.
- [x] A WAIT-mode Testcontainers test proves the core polling model (via `find`)
      correctly observes a real Redis-backed primary's completion.

## Blocked by

- Slice 014 — `IdempotencyStore` contract test suite

## Comments

- Implemented per ADR 0001's Redis key structure: one Hash per record, keyed
  by `{prefix}{sha256(method\0path\0principal\0keyValue)}`
  (`idempotency.redis.key-prefix`, default `idempotency:`, new
  `RedisStoreProperties`). `reserve` (`EXISTS`-gated `HSET`+`PEXPIRE`),
  `complete`, and `release` (both fence-token-checked `HGET` + write/`DEL`)
  are single Lua scripts each - Redis runs a script to completion with no
  other command interleaved, so no separate locking is needed. Lifecycle
  rides the key's own TTL (`PEXPIRE`); an expired key is simply absent from
  Redis, so - unlike Postgres - no conditional-supersession SQL is needed to
  let a fresh caller reclaim it. The response body is Base64-encoded (a Hash
  field is a Redis string; the body is arbitrary bytes) and its absence on an
  otherwise-completed record signals `response_unavailable`, mirroring
  `response_body IS NULL` in the Postgres store.
- A genuine connection failure surfaces as `StoreUnavailableException` via a
  new `StoreUnavailableException(String, Throwable)` constructor - catching
  both `DataAccessResourceFailureException` (connection refused/lost) and
  `QueryTimeoutException` (an already-established connection that stops
  responding, which is what a stopped Testcontainers container actually
  produces against Spring Boot's auto-configured Lettuce client in practice,
  confirmed by first writing the test and watching it fail with the wrong
  exception type before broadening the catch).
- All acceptance criteria are backed by tests against a real Testcontainers
  Redis, not simulated:
  - `RedisIdempotencyStoreContractTest` - the shared Slice 014 suite (TTL
    expiry, fence-token no-ops for both `complete` and `release`, not just
    `complete` as the issue text's Lua-script sentence names), plus a
    Redis-specific test proving a genuinely non-UTF-8 response body
    round-trips losslessly through the Base64 encoding.
  - `RedisIdempotencyStoreConcurrencyTest` - 20 real threads race `reserve()`
    on the same fresh key; exactly one wins, proving the Lua script is
    genuinely atomic under real concurrency, not just single-threaded-safe.
  - `RedisIdempotencyEndToEndTest` - MockMvc happy-path replay/collision
    through the real interceptor/engine/store stack.
  - `RedisIdempotencyStoreOutageTest` - a stopped container's `reserve()`
    call throws `StoreUnavailableException` (with a sanity-check reserve
    against the same store *before* stopping it, so the failure can't be a
    false positive from a connection that never worked).
  - `RedisIdempotencyOutageEndToEndTest` - the same stopped-container outage,
    end-to-end: the default posture fails open (handler still runs) and
    `onStoreFailure=CLOSED` fails closed (503, handler never invoked).
  - `RedisIdempotencyWaitModeTest` - a real `IdempotencyEngine` (not a mock)
    wired to a real Redis-backed store; a WAIT-mode caller's decision
    resolves to `Replay` only after genuinely polling past a primary that
    completes on a 300ms delay on another thread.
- Verified against a real Testcontainers Redis in this environment: `mvn test`
  from the repo root, all modules green (108 core + 18 redis + 29 postgres),
  redis module re-run three times back-to-back with zero flakes.
