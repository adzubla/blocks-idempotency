# Slice 022 — Unify connection-failure classification across stores

> Source: code-review smell scan (2026-07-16) · Type: AFK
> Status: wontfix

## What to build

Both real stores translate connection failures into `StoreUnavailableException`,
but with divergent shapes: `PostgresIdempotencyStore.asStoreUnavailableIfConnectionFailure`
(`PostgresIdempotencyStore.java:345-355`) uses an `instanceof` cascade, while
`RedisIdempotencyStore.guarded` (`RedisIdempotencyStore.java:168-175`) uses a
catch clause. Extract one shared classifier/wrapper in
`blocks-idempotency-core` that both stores delegate to, so recognizing a new
"this is a connection failure" case is a single edit instead of two.

## Acceptance criteria

- [ ] One shared connection-failure classifier (or wrapping helper) lives in
      `blocks-idempotency-core`.
- [ ] Both `PostgresIdempotencyStore` and `RedisIdempotencyStore` delegate to
      it for translating driver-level failures into `StoreUnavailableException`.
- [ ] Existing fail-open/fail-closed contract tests (including the
      `RedisSystemException` case from commit `210e6b3`) continue to pass
      unmodified.

## Blocked by

—

## Comments

- Filed from a code-smell review of the codebase (Duplicated Code, divergent
  shapes for the same concern).
- Declined (2026-07-17): drafted a proposal — a shared
  `ConnectionFailureClassifier.isConnectionFailure(Throwable, Class<?
  extends Throwable>...)` in `blocks-idempotency-core` that both stores'
  existing catch sites would call, with each store still passing its own
  driver-specific exception types (`CannotCreateTransactionException`/
  `TransactionSystemException` for Postgres, `RedisSystemException` for
  Redis) as varargs. Only `DataAccessResourceFailureException`/
  `QueryTimeoutException` are genuinely shared between the two stores today;
  the rest of each `instanceof`/catch list is store-specific and has no
  common supertype worth coupling to. Not worth the added indirection for
  that little actual duplication - the two "divergent shapes" mostly reflect
  a real difference (JDBC/transaction exceptions vs. Redis driver
  exceptions), not needless drift.
