# Slice 007 — Store-failure posture (fail-open / fail-closed)

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

When the store is unavailable, the resolved `onStoreFailure` posture decides:
**fail-open** (default) lets the request through unprotected; **fail-closed**
returns **503**. The in-memory fake gains an "unavailable" mode to simulate outages.
Per-endpoint override plus global default (via Slice 006).

## Acceptance criteria

- [x] Store unavailable + fail-open → handler executes unprotected (the library emits
      no 5xx).
- [x] Store unavailable + fail-closed → **503**, handler not executed.
- [x] Default posture is fail-open; a per-endpoint `CLOSED` overrides; the global
      default is respected.
- [x] `IdempotencyEngine` tests with an unavailable fake; MockMvc tests for both
      postures.

## Blocked by

- Slice 001 — header-strategy happy-path replay
- Slice 006 — policy resolution + global-default inheritance
