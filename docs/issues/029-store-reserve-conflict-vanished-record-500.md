# Slice 029 — `reserve()` re-read race throws an unmapped exception → raw 500

> Source: bug hunt (2026-07-17) · Type: AFK
> Status: needs-triage

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

This slice **exposes and documents** the defect with a disabled repro test; the
fix is deferred (filed unfixed by request). The likely fix is to treat a
lost-conflict-then-vanished record as "the key is now free" and **retry the
reservation** (it can win the second time), or at minimum route it through
`StoreUnavailableException` so the `onStoreFailure` posture applies instead of a
raw 500.

## Acceptance criteria

- [x] A disabled repro exists:
      `RedisIdempotencyStoreReserveRaceTest.reserveMapsAVanishedConflictingRecordToStoreUnavailableRatherThanARaw500`
      (`@Disabled`, pure Mockito, no container). Removing `@Disabled` fails today
      because `reserve()` throws `IllegalStateException`.
- [ ] A `reserve()` that loses the conflict but finds the record gone no longer
      throws an unmapped `IllegalStateException`; it either retries and wins, or
      surfaces as `StoreUnavailableException` (posture applies).
- [ ] The equivalent Postgres boundary race
      (`PostgresIdempotencyStore.java:176-177`) is fixed the same way.
- [ ] Existing store contract tests still pass for both stores.

## Blocked by

—

## Comments

- Filed from a code read of the shipped library (bug hunt, 2026-07-17). Filed
  **unfixed by request**: expose now, fix later.
- Same root cause in both real stores; Redis has by far the larger race window.
