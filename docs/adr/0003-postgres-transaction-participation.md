# Postgres store: the store owns the transaction, not the interceptor

## Status

accepted

## Context and decision

ADR 0001 and the PRD state `PostgresIdempotencyStore`'s core value proposition as:
the reservation `INSERT` runs **inside the effect's transaction** — the `INSERT`
**is** the lock. A concurrent request with the same key blocks on the unique index
until the first commits (→ replay) or rolls back (→ executes); a primary crash =
rollback = lock released natively, no `lock-ttl` needed.

The current architecture doesn't make this true by construction. `store.reserve()`
is called from `IdempotencyInterceptor.preHandle()`, which runs **before** the
resolved handler method — and before any `@Transactional` boundary on it — even
begins. There is no existing mechanism for the reservation's `JdbcTemplate` call to
join a transaction that doesn't exist yet.

**We decided: `PostgresIdempotencyStore` opens and closes the transaction itself,
around its own `reserve`/`complete`/`release` calls. `IdempotencyInterceptor` and
`idempotency-core` are unchanged.**

- **`reserve()`** manually opens a transaction (`PlatformTransactionManager
  .getTransaction(...)` — not `TransactionTemplate`, whose callback-commits-on-return
  shape doesn't fit a transaction that must outlive the method) and runs
  `INSERT ... ON CONFLICT (http_method, path, principal, idempotency_key) DO
  NOTHING` inside it.
  - **0 rows affected** (row already exists): nothing to hold open — commit/close
    immediately and return `Outcome.EXISTS`. No handler is going to run, so there's
    nothing for the transaction to span.
  - **1 row affected** (fresh reservation): **leave the transaction open**, bound to
    the thread via Spring's normal `TransactionSynchronizationManager` resource
    binding. Stash the `TransactionStatus` in a store-internal thread-local, keyed
    by the fence token (`ReservationResult.fenceToken()` is already an opaque
    `String` — no `IdempotencyStore` interface change needed). Return
    `Outcome.RESERVED`.
- **Handler executes** on the same thread. Its own `@Transactional` (default
  `REQUIRED` propagation) joins the already-active transaction automatically. A
  handler with **no** `@Transactional` at all still has its plain
  `JdbcTemplate`/JPA calls participate, since Spring's resource binding is
  thread-local and doesn't require the call site itself to be annotated.
- **`complete()`** (2xx path) runs the best-effort response `UPDATE` against the
  still-bound connection, then commits. `INSERT` + the handler's own writes + the
  response `UPDATE` all commit atomically.
- **`release()`** (non-2xx/exception path, Slice 004) just **rolls back** — no
  `DELETE` needed. Rollback natively undoes the `INSERT` along with everything
  else, which is a stronger and cheaper guarantee than an explicit `DELETE` run in
  a separate statement.

The thread-local entry is cleared, via `try`/`finally`, on the owning path of both
`complete()` and `release()`, so a pooled servlet thread never carries a stale
`TransactionStatus` into an unrelated request. On the fenced/no-op path — the
fence token no longer matches the current reservation — both methods return
without touching the thread-local: clearing it there would wipe out the real
current owner's freshly-bound scope instead of the caller's own (already-stale)
one.

## Why not "the interceptor opens the transaction"

The alternative floated in Slice 016 — `IdempotencyInterceptor` opens a transaction
in `preHandle` (via `PlatformTransactionManager`/`TransactionTemplate`) spanning
through to a commit/rollback in `afterCompletion` — was rejected for two reasons
beyond the scope-creep the issue already flagged:

- **It breaks the ADR 0001 module boundary.** `idempotency-core` currently has zero
  dependency on any store module — that's the point of the pluggable-store design.
  Giving the interceptor a `PlatformTransactionManager` means core would need a
  spring-tx/jdbc dependency, and every idempotent request would open a DB
  transaction — including `store="redis"` endpoints that never touch the database.
  That's a pooled connection checked out for no reason on every Redis-backed
  request.
- **The interceptor has no clean way to make it store-conditional.** Which store an
  endpoint uses isn't known until deep inside `engine.before()`; teaching the
  interceptor to branch on it would mean duplicating store-selection logic outside
  the engine, or growing new SPI surface just to answer "should I open a
  transaction."

Keeping the transaction inside `PostgresIdempotencyStore` sidesteps both: only
Postgres-backed requests pay for a transaction, `idempotency-core` stays store-
agnostic, and the transaction's span is exactly `reserve()` → handler →
`complete()`/`release()` — narrower than the interceptor-owned alternative, which
would have covered the full `preHandle`→`afterCompletion` cycle including response
capture.

## Considered Options

- **Interceptor-owned transaction spanning `preHandle`→`afterCompletion`**
  (rejected) — matches the mechanism sketched in Slice 016, but breaks the ADR 0001
  module boundary (core would gain a spring-tx dependency and open transactions for
  non-Postgres stores too) and extends the transaction over the full
  request/response cycle rather than just the handler.
- **Store-owned transaction, opened in `reserve()` and closed in
  `complete()`/`release()`** (chosen) — no change to `idempotency-core` or the
  `IdempotencyStore` interface; only Postgres-backed requests open a transaction;
  span is exactly the handler's execution.
- **A new `IdempotencyStore` SPI hook (e.g. `beginParticipation()`/
  `endParticipation()`)** (rejected) — would let the interceptor stay unaware of
  transaction mechanics while still being store-conditional, but adds interface
  surface for something Postgres can already achieve entirely internally via
  thread-bound resources. Revisit only if a future store needs the interceptor's
  help to hook into a transaction it doesn't fully own itself.

## Consequences

- `PostgresIdempotencyStoreAutoConfiguration` must inject `PlatformTransactionManager`
  alongside `JdbcTemplate` and pass it into `PostgresIdempotencyStore`'s
  constructor.
- **Async request handling (`Callable`/`DeferredResult`/WebFlux-style handlers) is
  not supported under `store="postgres"`.** Those can hop threads between
  `preHandle` and handler execution, which breaks the thread-bound transaction this
  design relies on. This should be documented as an explicit constraint, and is a
  new limitation this design introduces (the interceptor-owned alternative would
  have had the same problem, just less visibly).
- `PostgresIdempotencyStore.release()` no longer needs a `DELETE` statement — a
  plain rollback both frees the row and undoes any partial handler writes. Slice
  017's acceptance criteria should be read as "rollback" wherever it currently says
  "DELETE the reservation row."
- ADR 0002's WAIT-mode native-blocking path (`await()`) is unaffected — it's a
  separate blocking `SELECT`/`INSERT` on the waiter's own thread, orthogonal to the
  primary's transaction lifecycle described here.
- The thread-local handoff between `reserve()` and `complete()`/`release()` is an
  internal implementation detail of `PostgresIdempotencyStore` — it must not leak a
  `TransactionStatus` across requests if a thread is reused from the pool, so both
  terminal methods must clear it on their owning path. On the fenced/no-op path
  (the token no longer matches the current reservation) the entry must be left
  untouched, since it belongs to a different, live reservation that owns the
  thread-local now.
- **The reservation write is a conditional `ON CONFLICT ... DO UPDATE ... WHERE
  expires_at < clock_timestamp()`, not the bare `DO NOTHING` sketched above.**
  `DO NOTHING` silently no-ops on *any* conflicting row, stale or not — it can't
  let a fresh caller supersede a reservation whose owner crashed without a
  connection actually dying (e.g. the shared `IdempotencyStoreContractTest`'s
  "past its lock-ttl" test, which every store, Postgres included, must satisfy per
  Slice 017/014's acceptance criteria). The conditional `DO UPDATE` preserves the
  core guarantee (a *live* conflicting transaction still blocks the second
  `INSERT` natively, since evaluating the `WHERE` requires locking the row) while
  adding the ability to reclaim a row nobody is still holding open. Note it uses
  `clock_timestamp()`, not `now()`/`CURRENT_TIMESTAMP` — the latter is frozen for
  the lifetime of a transaction and would never observe elapsed wall-clock time
  from inside the still-open reservation transaction.
- **`lock_timeout` is now set** (Slice 018, `PostgresStoreProperties.lockTimeout`,
  default 2s), satisfying ADR 0001/0002's "bounded by `lock_timeout`". `reserve()`
  sets it (`SET LOCAL`) for its own transaction before attempting the reservation;
  a caught lock-not-available error (SQLState `55P03`) rolls back and is fenced
  through as an ordinary `IN_PROGRESS` record carrying the caller's own
  fingerprint — reusing the engine's existing `RejectReason.IN_PROGRESS`/WAIT
  handling unchanged, rather than adding new store-contract surface for this one
  case. This closes the gap noted above: a waiter no longer holds its thread
  indefinitely against a live-but-stuck holder. Accepted tradeoff: since the
  caller's own fingerprint is used (the real one can't be read without blocking
  further), a genuinely colliding fingerprint isn't detected in this narrow
  window - it degrades to an ordinary in-progress conflict instead of a 422,
  resolved for real once the row's lock frees up.
