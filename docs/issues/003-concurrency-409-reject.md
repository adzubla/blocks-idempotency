# Slice 003 — In-flight concurrency → 409 REJECT

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

Handle a concurrent duplicate that arrives while the primary is still **in-progress**.
The default `whenInProgress=REJECT` returns an immediate **409 + `Retry-After`**
(reason `in_progress`) to the second request, without holding a servlet thread. The
reservation attempt returns "exists / in-progress", which the engine maps to 409.

## Acceptance criteria

- [x] A reservation that finds an in-progress record yields an immediate **409** with
      `Retry-After` and reason `in_progress`.
- [x] The concurrent duplicate does **not** execute the effect.
- [x] Default behavior is REJECT (no waiting, no thread held).
- [x] `IdempotencyEngine` tests cover the in-progress branch; a MockMvc test (store
      primed in-progress, or genuinely concurrent calls) covers the 409.

## Blocked by

- Slice 001 — header-strategy happy-path replay
