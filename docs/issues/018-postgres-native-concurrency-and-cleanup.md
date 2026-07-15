# Slice 018 — Postgres store: native concurrency, safe self-promotion, expiration cleanup

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

The behavior that makes Postgres's concurrency model genuinely different from
the core's polling default (ADR 0002's accepted store divergence): a concurrent
request with the same key **blocks on the reservation `INSERT`'s unique index**
(bounded by `lock_timeout`) rather than polling `find()`. If the primary commits,
the blocked waiter's own `INSERT` then fails with `unique_violation` → it
observes the committed row and replays. If the primary **rolls back** (crash or
error), the waiter's blocked `INSERT` then **succeeds** — it safely
**self-promotes** to executor, since the failed primary's effect rolled back
alongside it, making it correct to re-do the effect (user story #35).

Also: since Postgres has no native TTL, a scheduled job deletes expired rows
(using the migration's `expires_at` index) so the table doesn't grow unbounded.

## Acceptance criteria

- [x] A concurrent duplicate blocks on the unique index (verified via a genuine
      concurrent-request test against Testcontainers Postgres, not simulated) up
      to `lock_timeout`, rather than going through the core's polling path.
- [x] When the primary commits, the waiter's blocked `INSERT` resolves to a
      replay of the committed response.
- [x] When the primary rolls back (simulated crash or thrown exception), the
      waiter's blocked `INSERT` succeeds and it executes the effect itself
      (self-promotion) - exactly once, not zero or twice.
- [x] A scheduled cleanup job deletes rows past `expires_at`.
- [x] This store-divergence behavior is documented as an accepted difference
      from the Redis/in-memory polling model (already noted in ADR 0002 -
      confirm the implementation matches what's written there).

## Blocked by

- Slice 017 — Postgres store: reserve/complete/release

## Comments

- Implemented. `PostgresIdempotencyStore.reserve()` now sets `lock_timeout`
  (`PostgresStoreProperties`, default 2s) on its own transaction before
  attempting the reservation `INSERT`; a caught `55P03` (lock_not_available)
  rolls back and fences through as an `IN_PROGRESS` record carrying the
  caller's own fingerprint, reusing the engine's existing
  `RejectReason.IN_PROGRESS`/WAIT handling. `PostgresIdempotencyCleanupJob`
  sweeps expired rows on its own schedule (`idempotency.postgres.cleanup.*`,
  default every 5m, `enabled=false` to opt out), wired via
  `PostgresIdempotencyStoreAutoConfiguration`.
- Acceptance criteria are backed by genuine two-thread/two-connection tests
  against Testcontainers Postgres, not simulated:
  - `PostgresNativeConcurrencyTest` (store level, latch-driven) - blocking
    itself, commit → replay, rollback → self-promotion, and the `lock_timeout`
    bound (a waiter gives up in ~300ms against a primary that never resolves).
  - `PostgresIdempotencyEndToEndTest` - two new tests drive genuinely
    concurrent MockMvc requests through the full interceptor/engine/store
    stack, proving "exactly once, not zero or twice" via the effect's own row
    count (not just invocation counters, which legitimately count the
    primary's failed attempt too).
  - `PostgresIdempotencyCleanupJobTest` - direct sweep, no-op-when-nothing-expired,
    and the real internal schedule firing on its own.
  - `PostgresIdempotencyCleanupJobAutoConfigurationTest` - the job is wired by
    default and genuinely absent when `cleanup.enabled=false`.
- ADR 0003's "consequences" section is updated to reflect `lock_timeout` now
  being set (previously flagged there as owed to this slice), including the
  accepted tradeoff that a lock-timeout fallback can't detect a genuine
  fingerprint collision in that narrow window. ADR 0002's store-divergence note
  needed no changes - it already matched the implementation.
- Verified end-to-end against a real Testcontainers Postgres in this
  environment (`mvn test` from the repo root, all modules green), including
  three repeated runs of the concurrency-sensitive test classes back-to-back
  with zero flakes.
