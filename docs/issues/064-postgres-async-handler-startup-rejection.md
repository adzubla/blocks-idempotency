# Slice 064 — Reject async `@Idempotent` handlers on the Postgres store at startup

> Source: system analysis (2026-11-19) · Type: AFK
> Status: done

## What to build

The README states that the Postgres store doesn't support async handlers
(`Callable`/`DeferredResult`/`CompletableFuture`) and that *"Nothing rejects
this combination at startup"*. When it happens, `afterCompletion` runs on a
different thread, `scope.get()` is `null`, and `complete()` silently no-ops.
The client receives the handler's 2xx, but the effect is never committed.

The README understates the blast radius: the open transaction (and its pooled
connection) stays bound to the **pooled servlet thread**, so the next unrelated
request on that thread can join it. [Slice 033](033-postgres-dangling-reservation-scope.md)
tracks hardening against that leaked scope after the fact. This slice is the
cheap preventive half: `IdempotentHandlerValidator` already walks every
`@Idempotent` handler and resolves its store, so it can reject the known-async
return types at startup.

## Acceptance criteria

- [x] Startup fails with a clear message when an `@Idempotent` handler resolving
      to the `postgres` store returns `Callable`, `DeferredResult`,
      `CompletableFuture`/`CompletionStage`, `WebAsyncTask`, or `StreamingResponseBody`/`ResponseBodyEmitter`.
- [x] The same handlers on the `redis`/`in-memory` stores still start normally.
- [x] The README caveat is updated to describe the startup rejection.

## Blocked by

—

## Comments

- 2026-11-20 — Implemented as a store capability rather than a check on the
  `postgres` qualifier, since the web module doesn't depend on the Postgres
  module. The SPI gained `IdempotencyStore#supportsAsyncHandlers()` (default
  `true`), and `PostgresIdempotencyStore` returns `false`.
  `IdempotentHandlerValidator` fails startup for an async return type on such
  a store: `Callable`, `WebAsyncTask`, `DeferredResult`, `CompletionStage`,
  `ResponseBodyEmitter`, `StreamingResponseBody`, reactive types via
  `ReactiveAdapterRegistry`, and any of these inside `ResponseEntity`/`HttpEntity`.
  Tested at the agreed seam, `IdempotentHandlerValidatorTest`, with a fake
  store reporting no async support. No Postgres-module startup test (that seam
  wasn't selected). README §5.1/§6.4 and ADR 0003 updated.
