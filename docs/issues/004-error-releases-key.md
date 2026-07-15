# Slice 004 — Error releases the key

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

Only **2xx** responses are cached. When the handler returns a non-2xx (or throws),
the engine **releases** the reservation so a genuine retry re-executes rather than
replaying an error — a transient failure never freezes into a permanent one.

## Acceptance criteria

- [x] A non-2xx response releases the key (the record is removed).
- [x] A retry after an error **re-executes** the handler (no cached error is
      replayed).
- [x] 2xx responses are still cached (Slice 001 unaffected).
- [x] `IdempotencyEngine` tests cover success-caches vs. error-releases; a MockMvc
      test covers error-then-retry.

## Blocked by

- Slice 001 — header-strategy happy-path replay
