# Slice 019 — `await()` as a real `IdempotencyStore` operation, with a native Postgres override

> Source: docs/prd/idempotency-library.md · Type: AFK
> Status: ready-for-agent

## What to build

`IdempotencyStore#await` (ADR 0002 WAIT mode) stops being a stub that throws
`UnsupportedOperationException`, with WAIT-mode polling duplicated inline in
`IdempotencyEngine` (Slice 009). Instead:

- `IdempotencyStore#await` gets a real default implementation — the polling
  loop moved out of `IdempotencyEngine` and into the interface, with the
  jitter math extracted into an independently-testable `PollJitter`.
  `IdempotencyEngine.waitForCompletion` delegates to it instead of running
  its own loop.
- `PostgresIdempotencyStore` overrides `await()` with a genuine blocking wait,
  reusing the same `INSERT ... ON CONFLICT` primitive `reserve()` already uses
  (a bare `SELECT ... FOR UPDATE` can't do this — an in-flight, uncommitted
  `INSERT` is invisible to other transactions under MVCC and would return
  instantly instead of waiting). It resolves the instant the primary commits
  or rolls back, not on the next poll tick, and never self-promotes: if the
  conflict check would have "won" the row (primary released/crashed), that
  win is rolled back immediately and reported as `Optional.empty()`, matching
  WAIT mode's no-self-promotion contract (self-promotion stays exclusive to
  `reserve()`'s own blocking path, Slice 018).
- `idempotency.poll-interval`/`idempotency.poll-jitter` become configurable
  (previously hardcoded ~100ms/50ms in `IdempotencyEngine`), documented in the
  README; Postgres ignores both, since native blocking replaces the poll
  cadence entirely.

## Acceptance criteria

- [x] `IdempotencyStore#await` has a real default (polling) implementation;
      `IdempotencyEngine` no longer duplicates the loop.
- [x] `PollJitter` is extracted and independently unit-tested.
- [x] `PostgresIdempotencyStore#await` overrides with a native blocking wait,
      verified via a genuine two-thread test against Testcontainers Postgres
      (not simulated) proving it resolves on the primary's actual
      commit/rollback rather than a poll tick.
- [x] `await()` never self-promotes: a "won" conflict check is rolled back
      immediately rather than kept.
- [x] `idempotency.poll-interval`/`idempotency.poll-jitter` are configurable
      and documented; Postgres's divergence (ignores both) is documented too.
- [x] The shared `IdempotencyStoreContractTest` suite (Slice 014) exercises
      `await()` — completes-while-waiting, released-while-waiting, and
      wait-timeout-elapses-first — so all three stores are held to the same
      `await()` contract, not just Postgres's own ad hoc test.

## Blocked by

- Slice 009 — WAIT mode (core polling) + `response_unavailable` (the loop this replaces)
- Slice 018 — Postgres store: native concurrency, safe self-promotion, expiration cleanup (the `INSERT ... ON CONFLICT` primitive this reuses)

## Comments

- Implemented directly (commit `b6bfa0a`) without a prior issue file — this
  slice was filed retroactively after a docs/implementation alignment check
  flagged the gap: Slice 009's scope note deferred "Postgres native locking"
  to a later batch, but neither Slice 018 (scoped to `reserve()`-level
  blocking only) nor any other issue described this separate `await()`
  override.
- The alignment check also found the PRD's Testing Decisions section
  (`docs/prd/idempotency-library.md`) claiming the shared
  `IdempotencyStoreContractTest` suite covers "wait/await semantics," which
  wasn't true — Slice 014's acceptance criteria never listed it and the suite
  had zero references to `await`. Closed as part of this slice: three new
  contract tests exercise `await()` generically across `InMemoryIdempotencyStore`,
  `RedisIdempotencyStore`, and `PostgresIdempotencyStore`. They deliberately run
  the `await()` call on a thread separate from the one that called `reserve()`
  — `PostgresIdempotencyStore` binds a reservation's transaction to the
  reserving thread, so an `await()` sharing that thread would silently join
  the reservation's own still-open transaction instead of genuinely blocking
  on it.
- Verified: `mvn -pl blocks-idempotency-core -am test` (in-memory contract
  suite, 13/13 green, including the 3 new `await()` tests) in this
  environment. The Redis/Postgres contract suites could not be *executed*
  here at first — no Docker available in this sandbox (same limitation noted
  in Slice 017's comments).
- **Once Docker became available, the new Postgres `await()` contract test
  caught a genuine bug, deterministically (4/4 runs), not a flake.**
  `PostgresIdempotencyStore#await`'s lock-timeout branch (waitTimeout
  exhausted while genuinely still blocked on the primary's open reservation)
  called `findRecord(key)` as a fallback — but a plain `SELECT` can't see an
  in-flight, uncommitted `INSERT` under MVCC, so it always returned empty
  instead of the still-in-progress record. The engine then reported
  `RejectReason.RELEASED` instead of `RejectReason.TIMEOUT` in
  `Idempotency-Reject-Reason` — wrong signal to the client (the primary
  hadn't failed or released anything, it was still running).
  **Fixed**: that branch now synthesizes `Optional.of(new
  IdempotencyRecord(RecordState.IN_PROGRESS, null, null))` instead of
  querying, mirroring `reserve()`'s own lock-timeout branch, which already
  solves the identical MVCC-visibility problem the same way. No fingerprint
  is available to carry there (unlike `reserve()`, `await()` has no
  fingerprint parameter) - none is needed, since the engine's WAIT-mode
  timeout branch only inspects `IdempotencyRecord#isCompleted`.
- Full suite verified green after the fix: `mvn test` from the repo root,
  120 + 21 + 41 tests across `core`/`redis`/`postgres`, 3 clean repeated runs
  of the Postgres contract suite specifically to rule out a flake before
  concluding it was a real bug.
