# Slice 002 — Payload collision → 422

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

Detect a key reused with a **different payload**. On reservation the engine computes
a fingerprint — SHA-256 of method + path + the **normalized** (canonical
key-ordered) request body — and stores it with the record. On a repeat, if the
incoming fingerprint differs from the stored one, respond **422** (terminal); if it
matches, replay as in Slice 001.

## Acceptance criteria

- [x] Fingerprint covers method + path + normalized JSON body; cosmetic key
      reordering does **not** change it.
- [x] Same key + same body → replay (Slice 001 behavior).
- [x] Same key + different body → **422**, no replay, effect not re-run.
- [x] `IdempotencyEngine` tests cover both match and mismatch; a MockMvc test covers
      the 422 path.

## Blocked by

- Slice 001 — header-strategy happy-path replay
