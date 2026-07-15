# Slice 008 — Expiration = new key

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

An expired record behaves as a brand-new key (re-executes), with **no** "expired"
error. Default TTL is 24h, overridable per endpoint. The in-memory fake uses an
injectable clock so tests can advance time past the TTL.

## Acceptance criteria

- [x] Within the TTL a repeat replays; after the TTL the same key **re-executes**.
- [x] No special "expired" status — expiry is indistinguishable from a new key.
- [x] TTL resolves from the annotation `ttl` or the global default (Slice 006).
- [x] `IdempotencyEngine` tests with a controllable clock; the store fake honors TTL.

## Blocked by

- Slice 001 — header-strategy happy-path replay
- Slice 006 — policy resolution + global-default inheritance
