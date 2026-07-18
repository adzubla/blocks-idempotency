# Slice 031 — Default `await()` ignores the injectable Clock and sleeps before its first check

> Source: bug hunt (2026-07-17) · Type: AFK
> Status: needs-triage

## What to build

`IdempotencyStore`'s default `await` (`store/IdempotencyStore.java:57-72`) has
two related weaknesses:

1. **No fast-path initial check.** The loop computes the remaining budget,
   `PollJitter.sleep(...)`, and only *then* calls `find()`. A duplicate that
   arrives just after the primary completed still pays a full poll interval of
   latency before the already-available answer is returned.
2. **Wall-clock deadline vs. injectable Clock.** The deadline is computed from
   `Instant.now()`, but `InMemoryIdempotencyStore` expiry (`find`/`reserve`) is
   driven by an injected `Clock` documented as "injectable so tests can advance
   time" (`store/InMemoryIdempotencyStore.java:23-24`). The two notions of time
   diverge, so `await`-vs-expiry interactions can't be driven deterministically
   and can behave differently under a fake clock than in production.

This slice **exposes and documents** the defect with a disabled repro test
(targeting #1, which is crisply deterministic); the fix is deferred (filed
unfixed by request). A fix would add an initial `find()` before the first sleep,
and thread the store's `Clock` through `await`'s deadline math.

## Acceptance criteria

- [x] A disabled repro exists:
      `IdempotencyStoreDefaultAwaitTest.awaitReturnsPromptlyForAnAlreadyCompletedKeyInsteadOfSleepingAPollInterval`
      (`@Disabled`, deterministic inline store). Removing `@Disabled` fails today
      because `await` sleeps a full poll interval before its first `find()`.
- [ ] `await` checks `find()` once before sleeping, so an already-terminal key
      returns without an initial poll delay.
- [ ] `await`'s deadline uses the store's `Clock` (consistent with expiry),
      verifiable with a fake clock.
- [ ] Existing WAIT-mode / contract `await` tests still pass.

## Blocked by

—

## Comments

- Filed from a code read of the shipped library (bug hunt, 2026-07-17). Filed
  **unfixed by request**: expose now, fix later.
- The Clock-divergence half mostly bites tests (production uses `systemUTC`); the
  missing initial check is a real per-duplicate latency cost in WAIT mode.
