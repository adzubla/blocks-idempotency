# Slice 020 — Extract a shared header JSON codec for the store modules

> Source: code-review smell scan (2026-07-16) · Type: AFK
> Status: wontfix

## What to build

`RedisIdempotencyStore.toJson`/`parseHeaders` (`RedisIdempotencyStore.java:212-230`)
is byte-for-byte identical to `PostgresIdempotencyStore.toJson`/`parseHeaders`
(`PostgresIdempotencyStore.java:424-441`). Extract a single `HeaderJsonCodec`
(or equivalent) in `blocks-idempotency-core`, and have both stores delegate to
it instead of carrying their own copies.

## Acceptance criteria

- [ ] A single header (de)serialization implementation lives in
      `blocks-idempotency-core`.
- [ ] `RedisIdempotencyStore` and `PostgresIdempotencyStore` both delegate to
      it; the duplicated private methods are removed.
- [ ] Existing store contract tests continue to pass unmodified (behavior is
      unchanged — this is a pure dedup).

## Blocked by

—

## Comments

- Filed from a code-smell review of the codebase (Duplicated Code).
