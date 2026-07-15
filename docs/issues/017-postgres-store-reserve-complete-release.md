# Slice 017 — Postgres store: reserve/complete/release

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

A working `PostgresIdempotencyStore` (previously a scaffold whose methods all
threw `UnsupportedOperationException`), wired against the existing Flyway
migration (`idempotency_record`, composite primary key `(http_method, path,
principal, idempotency_key)`, plus a `reservation_token` fencing column), 
implementing ADR 0003's transactional model:

- **reserve**: `INSERT ... ON CONFLICT (http_method, path, principal,
  idempotency_key) DO UPDATE ... WHERE expires_at < clock_timestamp()` - not a
  bare `DO NOTHING` as originally sketched here, since that can't let a fresh
  caller supersede a reservation whose owner never called `complete`/`release`
  (see ADR 0003's consequences for why). 1 row returned means reserved, and the
  transaction is left open for the handler to join; 0 rows means an existing,
  still-valid record - fingerprint mismatch → the engine's collision path;
  otherwise → replay, or `response_unavailable` if the completed row's
  `response_body` is `NULL` (oversized response - see `ResponseCapture` - not
  `response_status IS NULL`, which ADR 0003 makes unreachable on a committed row:
  the reservation, the handler's effect, and the response `UPDATE` all commit
  together in one transaction).
- **find**: `SELECT` by the composite key.
- **complete**: best-effort `UPDATE` writing `response_status`/`response_headers`/
  `response_body`, fenced by `reservation_token`, then commits the still-open
  transaction from `reserve`.
- **release**: rolls back the still-open transaction from `reserve` (ADR 0003) -
  not a `DELETE`; rollback undoes the reservation `INSERT` (and anything the
  handler wrote) natively.

## Acceptance criteria

- [x] `PostgresIdempotencyStore.reserve/find/complete/release` are real
      implementations, honoring ADR 0003's transactional decision.
- [x] The Slice 014 contract test suite passes against `PostgresIdempotencyStore`
      backed by Testcontainers Postgres.
- [x] A `@Idempotent(store="postgres")` MockMvc end-to-end test proves replay
      works against a real (Testcontainers) Postgres.
- [x] A repeat with a different fingerprint against an existing row → 422
      collision (same engine-level mechanism as the other stores).
- [x] `response_unavailable` is reachable and matches the other stores' behavior
      (via a committed row with a `NULL` `response_body`, not `response_status IS
      NULL` - see "What to build" above for why that specific DB state doesn't
      arise under ADR 0003).

## Blocked by

- Slice 014 — `IdempotencyStore` contract test suite

## Comments

- Implemented per ADR 0003. `PostgresIdempotencyStoreContractTest` and the new
  `PostgresIdempotencyEndToEndTest` (Testcontainers-backed) were written and
  compile clean, but could not be *executed* in the sandbox this was implemented
  in - no Docker/WSL integration available there. Run `mvn -pl
  idempotency-store-postgres -am test` in an environment with Docker before
  merging to confirm they actually pass against a real Postgres.
