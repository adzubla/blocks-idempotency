# Slice 016 — HITL: how does the Postgres reservation join the effect's own transaction?

> Source: docs/prd/idempotency-library.md · Type: HITL

## What to build

A design decision, not code. ADR 0001 and the PRD both state the Postgres
store's core value proposition as: "the reservation `INSERT` runs inside the
effect's transaction" — the `INSERT` **is** the lock; a concurrent request with
the same key blocks on the unique index until the first commits (replay) or
rolls back (executes); primary crash = rollback = lock released natively, no
`lock-ttl` needed. The existing migration's own SQL comment repeats this
assumption verbatim.

The current architecture doesn't make this true by construction. `store.reserve()`
is called from `IdempotencyInterceptor.preHandle()`, which runs **before** the
resolved handler method — and before any `@Transactional` boundary on it — even
begins. There is no existing mechanism for the reservation's `JdbcTemplate` call
to join a transaction that doesn't exist yet.

A workable mechanism (not yet decided, needs a human call): have the interceptor
itself open a transaction (e.g. via `PlatformTransactionManager`/
`TransactionTemplate`) spanning `preHandle` through the handler invocation
through the `complete()` write in `afterCompletion`, committing there on a 2xx (or
rolling back on error/non-2xx, matching the existing release-on-error behavior).
A handler's own `@Transactional` (default `REQUIRED` propagation) would then join
that already-active transaction automatically, and even a handler with no
`@Transactional` at all would have its own JDBC/JPA calls participate too, since
Spring's resource binding is thread-local and doesn't require the call site
itself to be annotated.

The tradeoff worth a human sign-off: this measurably extends the transaction's
duration to cover the whole request/response cycle (including response
serialization in `afterCompletion`), not just the handler method body — a real
change to normal Spring transaction-scoping expectations that a per-endpoint
opt-in library probably shouldn't make silently.

## Acceptance criteria

- [x] A decision is recorded (e.g. as an ADR addendum or a new ADR) on the
      transaction-boundary mechanism `PostgresIdempotencyStore` will rely on.
      Recorded as `docs/adr/0003-postgres-transaction-participation.md`
      (Status: accepted).
- [x] The decision explicitly addresses: how a handler's own `@Transactional`
      (or lack thereof) interacts with the reservation's transaction; whether
      the transaction spans the full request/response cycle or something
      narrower; and how rollback-on-error interacts with the existing
      error-releases-the-key behavior (Slice 004). All three are covered in
      ADR 0003's "Context and decision" section (store-owned transaction,
      opened in `reserve()` and closed in `complete()`/`release()`, scoped to
      just the handler's execution, with `release()` reducing to a plain
      rollback instead of an explicit `DELETE`).

## Blocked by

None - can start immediately (a design conversation, not implementation).
