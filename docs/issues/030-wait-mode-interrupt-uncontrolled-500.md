# Slice 030 — Interrupt during WAIT-mode polling escapes as an uncontrolled 500

> Source: bug hunt (2026-07-17) · Type: AFK
> Status: done

## What to build

When a duplicate is parked in WAIT mode, the engine polls the store via
`PollJitter.sleep`. On `InterruptedException`, `PollJitter.sleep` re-sets the
interrupt flag and throws `IllegalStateException`
(`store/PollJitter.java:22-25`). But `IdempotencyEngine.waitForCompletion`
catches only `StoreUnavailableException` (`engine/IdempotencyEngine.java:101-105`),
so the `IllegalStateException` propagates out of `before()` → `preHandle` as an
**uncontrolled 500** rather than a controlled idempotency outcome.

Any request thread interrupted mid-wait (thread-pool shutdown, client
disconnect handling, timeout tooling) turns into a raw 500.

This slice **exposes and documents** the defect with a disabled repro test; the
fix is deferred (filed unfixed by request). A fix would decide the intended
WAIT-interrupt semantics (e.g. treat as a controlled reject/timeout outcome, or
propagate `InterruptedException` cleanly) while preserving the thread's interrupt
status.

## Acceptance criteria

- [x] A disabled repro exists:
      `IdempotencyEngineWaitInterruptTest.interruptDuringWaitDoesNotLeakAnUncontrolledIllegalStateException`
      (`@Disabled`, deterministic via a pre-set interrupt). Removing `@Disabled`
      fails today because a bare `IllegalStateException` escapes `before()`.
- [x] An interrupt during WAIT-mode polling produces a controlled outcome (not a
      raw `IllegalStateException`/500).
- [x] The thread's interrupt status is preserved for the caller.
- [x] Existing engine and WAIT-mode tests still pass.

## Blocked by

—

## Comments

- Filed from a code read of the shipped library (bug hunt, 2026-07-17). Filed
  **unfixed by request**: expose now, fix later.
- 2026-11-20 — Implemented. An interrupt now ends the wait early, the same way
  an elapsed `waitTimeout` does. `PollJitter.sleep` returns `false` on
  interrupt and re-sets the flag instead of throwing. The default `await()`
  then stops waiting and returns the last record it observed, which is still
  `IN_PROGRESS`, without calling the store again: Lettuce would turn I/O on
  an interrupted thread into `RedisCommandInterruptedException`. The engine
  maps that to its existing `Reject(TIMEOUT)`, a `409 timeout` with
  `Retry-After`, and logs the interrupt at `DEBUG`. The interrupt status stays
  set for the container. Rejecting rather than throwing was chosen because the
  waiter never ran the effect, so "gave up waiting, retry" is accurate. The
  repro was rewritten as
  `anInterruptDuringWaitEndsTheWaitAsAControlledTimeoutRejection` and is no
  longer `@Disabled`. Postgres's native `await()` doesn't sleep, so it isn't
  affected.
