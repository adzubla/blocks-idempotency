# Slice 029 — `reserve()` re-read race throws an unmapped exception → raw 500

> Source: bug hunt (2026-07-17) · Type: AFK

## What to build

Both real stores implement `reserve()` as: check for a conflict atomically,
then, if the conflict was lost, **re-read** the existing record in a separate
step to return it. If the conflicting record disappears in the window between
those two steps (its lock-ttl expired, or it was released), the re-read returns
empty and the code throws a bare `IllegalStateException`:

- Redis — `RedisIdempotencyStore.java:129-130` (`findRecord(...).orElseThrow(...)`);
  the two steps are separate network round-trips, so the window is **wide**
  (short lock-ttl vs. two round-trips). `guarded(...)` (`:165-172`) only maps
  `DataAccessResourceFailureException | QueryTimeoutException | RedisSystemException`,
  so the `IllegalStateException` escapes uncaught → **raw 500, bypassing the
  configured `onStoreFailure` posture**.
- Postgres — `PostgresIdempotencyStore.java:176-177`: same shape, narrower
  window — a boundary race between the upsert's `expires_at < clock_timestamp()`
  (`:99`) and the select's `expires_at > clock_timestamp()` (`:106`). The row can
  be "not stale enough to supersede" yet "already expired to read." Also mapped
  to `IllegalStateException` rather than a store-unavailable outcome.

The in-memory store is immune (a single atomic `compute` reads the existing
record inside the critical section), so this is a store-level defect only.

**Fixed** by treating a lost-conflict-then-vanished record as "the key is free
now" and **retrying** the reservation (bounded, `MAX_RESERVE_ATTEMPTS = 8`): the
retry wins once the key is free (Redis re-`EXISTS`; Postgres's later
`clock_timestamp()` supersedes the stale row). Only if it keeps racing past the
bound does `reserve()` surface `StoreUnavailableException` so the
`onStoreFailure` posture applies — never a raw 500. The bare
`IllegalStateException` is gone from both stores.

## Acceptance criteria

- [x] A `reserve()` that loses the conflict but finds the record gone no longer
      throws an unmapped `IllegalStateException`; it retries and wins, or (if the
      race persists) surfaces as `StoreUnavailableException` (posture applies).
      Covered by `RedisIdempotencyStoreReserveRaceTest` (retry-wins +
      keeps-racing) and `PostgresIdempotencyStoreReserveRaceTest` (same, plus a
      rollback assertion), both pure Mockito.
- [x] The equivalent Postgres boundary race is fixed the same way
      (`MAX_RESERVE_ATTEMPTS` retry loop in `PostgresIdempotencyStore.reserve`).
- [x] Existing store contract tests still pass for both stores — full Redis
      (24) and Postgres (44) module suites green, including the shared contract,
      native-concurrency, and end-to-end tests.

## Blocked by

—

## Comments

- Filed from a code read of the shipped library (bug hunt, 2026-07-17).
- Same root cause in both real stores; Redis has by far the larger race window.
- Implemented (2026-07-17): bounded retry loop in `RedisIdempotencyStore.reserve`
  and `PostgresIdempotencyStore.reserve`, falling back to
  `StoreUnavailableException` past `MAX_RESERVE_ATTEMPTS`. The former `@Disabled`
  repro is now enabled and split into retry-wins and keeps-racing cases for both
  stores; full store suites pass (`mvn test -pl blocks-idempotency-store-redis,blocks-idempotency-store-postgres -am`).
