# Slice 024 — Give `EffectiveKey` an ordered accessor instead of manual unpacking

> Source: code-review smell scan (2026-07-16) · Type: AFK
> Status: needs-triage

## What to build

`PostgresIdempotencyStore.keyArgs()` (`PostgresIdempotencyStore.java:391-393`)
manually unpacks `EffectiveKey`'s four fields into an `Object[]` for every SQL
statement that needs them. Let `EffectiveKey` expose its own ordered component
accessor (e.g. `toSqlArgs()` or similar) so the unpacking logic lives with the
type it belongs to, not scattered across call sites.

## Acceptance criteria

- [ ] `EffectiveKey` exposes an ordered-components accessor.
- [ ] `PostgresIdempotencyStore.keyArgs()` (or its replacement) delegates to
      it instead of manually indexing fields.
- [ ] Postgres store contract tests continue to pass unmodified.

## Blocked by

—

## Comments

- Filed from a code-smell review of the codebase (Data Clumps).
