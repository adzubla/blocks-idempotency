# Slice 064 — Reject async `@Idempotent` handlers on the Postgres store at startup

> Source: system analysis (2026-11-19) · Type: AFK
> Status: needs-triage

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

- [ ] Startup fails with a clear message when an `@Idempotent` handler resolving
      to the `postgres` store returns `Callable`, `DeferredResult`,
      `CompletableFuture`/`CompletionStage`, `WebAsyncTask`, or `StreamingResponseBody`/`ResponseBodyEmitter`.
- [ ] The same handlers on the `redis`/`in-memory` stores still start normally.
- [ ] The README caveat is updated to describe the startup rejection.

## Blocked by

—
