# Slice 021 — Share key-digest computation between `Fingerprint` and Redis key building

> Source: code-review smell scan (2026-07-16) · Type: AFK

## What to build

`RedisIdempotencyStore.redisKey()` (`RedisIdempotencyStore.java:196-210`)
rebuilds the same `method\0path\0principal\0value` SHA-256 digest shape that
`Fingerprint` (`core/.../fingerprint/Fingerprint.java:30-42`) already computes.
Give `EffectiveKey` (or `Fingerprint`) a `digestBytes()`/`canonicalString()`
method that both call, and have `redisKey()` use it instead of re-deriving the
digest inline.

## Acceptance criteria

- [x] The digest-shape logic exists in exactly one place, exposed from
      `EffectiveKey`/`Fingerprint`.
- [x] `RedisIdempotencyStore.redisKey()` calls the shared method; no inline
      re-derivation remains.
- [x] Redis store contract tests continue to pass unmodified — the actual
      Redis key values produced must not change (would break existing stored
      records' addressability).

## Blocked by

—

## Comments

- Filed from a code-smell review of the codebase (Duplicated Code).
- Implemented: `Fingerprint.digestBytes(byte[]... parts)` is the shared
  NUL-join + SHA-256 primitive, used by both `Fingerprint.sha256()` and the
  new `EffectiveKey.digestBytes()` (method/path/principal/value).
  `RedisIdempotencyStore.redisKey()` now calls `key.digestBytes()` and no
  longer re-derives the digest inline. Verified byte-for-byte against the old
  inline derivation (test in `EffectiveKeyTest`) so stored Redis key values
  are unchanged. `/code-review` (Standards + Spec) came back clean after one
  javadoc wording fix; full `mvn test` passes. Commit `53a9f10`.
