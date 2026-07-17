# Slice 025 — Introduce a `StoreQualifier` value type

> Source: code-review smell scan (2026-07-16) · Type: AFK
> Status: needs-triage

## What to build

The store qualifier is a bare `String` threaded through `Idempotent.store()`,
`IdempotencyPolicy.store()`, `IdempotencyEngineRegistry`'s map keys, and each
store's `QUALIFIER` constant (`PostgresIdempotencyStore.java:85`,
`RedisIdempotencyStore.java:66`). A typo in any of these compiles fine and
only fails at runtime/startup. Introduce a small `StoreQualifier` value type
(or reuse the existing startup validation path, Slice 010, to check it
consistently) so the concept has one home.

## Acceptance criteria

- [ ] `StoreQualifier` (or equivalent) type exists in
      `blocks-idempotency-core`.
- [ ] `Idempotent.store()`, `IdempotencyPolicy.store()`,
      `IdempotencyEngineRegistry`, and each store's qualifier constant use it
      instead of a bare `String`.
- [ ] Startup validation (Slice 010) still catches an unknown/mistyped
      qualifier fail-fast, now via the new type rather than string comparison.
- [ ] Existing config/wiring tests continue to pass unmodified.

## Blocked by

—

## Comments

- Filed from a code-smell review of the codebase (Primitive Obsession).
