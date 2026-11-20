# Slice 033 — Harden against a dangling Postgres reservation scope surviving thread reuse

> Source: bug hunt (2026-07-17) · Type: AFK
> Status: done

## What to build

The winning `reserve()` leaves its transaction open and thread-bound via a
`ThreadLocal<ReservationScope>` (`PostgresIdempotencyStore.java:168-170`,
`bindScope` `:382-385`), relying on `complete()`/`release()` — driven by the
interceptor's `afterCompletion` — to resolve it. In the supported synchronous
servlet model that resolution is guaranteed, so this is safe.

But if a reservation is ever left **unresolved** on a pooled thread (a
thread-hopping/async handler — explicitly *"Not yet supported"*,
`PostgresIdempotencyStore.java:75-78`, ADR 0003 — or any path where the
interceptor never runs), the stale scope survives onto the next request that
reuses the thread. The next winning `reserve()` on that thread joins the still-open
transaction (default `REQUIRED` propagation), and `bindScope` deliberately keeps
the **outer** transaction (correct for the intended same-request self-supersede
case, but indistinguishable from a leaked prior scope). Completing that fresh
reservation then **commits the dangling one as a side effect** — the earlier key
leaks as a committed `IN_PROGRESS` row that blocks genuine retries until it
expires, even though its request was never resolved. Both the DB
connection/transaction and the `ThreadLocal` are also leaked until then.

Redis has no per-thread state, so this asymmetry is Postgres-only. The store
already ships `abandonDanglingReservationForTests()` precisely because this
dangling state is otherwise unrecoverable — a signal that production hardening is
worth considering.

This slice **documents and exposes** the gap with a disabled repro; the fix is
deferred. Candidate hardening (for triage):
- Detect a pre-existing scope at `reserve()` entry that belongs to a *different*
  key and treat it as a leak (abandon/roll back) rather than silently joining.
- Guard `complete()`/`release()`/`bindScope` so a scope can only ever resolve the
  reservation that created it.

## Acceptance criteria

- [x] A disabled repro exists:
      `PostgresDanglingReservationScopeTest.completingAFreshReservationDoesNotCommitADanglingOneLeftOnTheSameThread`
      (`@Disabled`, `@Testcontainers`, uses the `abandonDanglingReservationForTests`
      seam for cleanup). Removing `@Disabled` fails today because the dangling
      key leaks as a committed reservation.
- [x] A reservation left unresolved on a reused thread cannot be committed by a
      later, unrelated reservation on that thread.
- [x] No stale `ThreadLocal` scope or open transaction bleeds across requests on
      a pooled thread.
- [x] The intended same-request self-supersede behavior (contract test) still
      passes.

## Blocked by

—

## Comments

- Filed from a code read of the shipped library (bug hunt, 2026-07-17). Framed
  as **defense-in-depth hardening**, not a bug in the supported synchronous
  model — the trigger (unresolved reservation surviving a thread) is currently
  out-of-contract (async handling, ADR 0003). Filed **unfixed by request**.
- 2026-11-20 — Implemented the first candidate. `ReservationScope` now records
  its `EffectiveKey`. At entry, `reserve()` rolls back a leftover scope for a
  *different* key, logs a `WARN` and clears the `ThreadLocal`, then opens a
  fresh transaction. That frees the leaked key, returns its connection, and
  stops the new reservation's `complete()` from committing it. A leftover scope
  for the *same* key is still joined, because that is the contract suite's
  nested re-reservation. `PostgresDanglingReservationScopeTest` is no longer
  `@Disabled`. It also asserts that no transaction resource stays bound to the
  thread, that the leaked key can be reserved again, and that same-key
  nesting still behaves as before. Remaining gap: a leaked scope is only
  reclaimed by the next `reserve()` on that thread, so non-idempotent work that
  runs there first still joins it. Since 064, async handlers on Postgres are
  rejected at startup, so the trigger is out of contract.
