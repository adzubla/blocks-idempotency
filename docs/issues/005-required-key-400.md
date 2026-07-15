# Slice 005 — Required key → 400 / `keyRequired` override

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

`keyRequired` (default **true**) enforces the key's presence: an absent header →
**400**, handler not executed. `keyRequired=false` makes protection a client opt-in:
an absent header lets the request **pass through unprotected** (executes normally,
nothing cached). A present header behaves as Slice 001.

## Acceptance criteria

- [x] Missing header with `keyRequired=true` → **400**, handler not executed.
- [x] Missing header with `keyRequired=false` → handler executes unprotected, no
      record created.
- [x] Present header behaves as Slice 001.
- [x] Interceptor/engine tests + MockMvc tests for both postures.

## Blocked by

- Slice 001 — header-strategy happy-path replay
