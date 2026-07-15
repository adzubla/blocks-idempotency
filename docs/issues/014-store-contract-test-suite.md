# Slice 014 — `IdempotencyStore` contract test suite

> Source: docs/prd/idempotency-library.md · Type: AFK
> Status: ready-for-agent

## What to build

A shared, parameterized behavioral test suite that defines the `IdempotencyStore`
contract once and can be run against any implementation (see the PRD's Testing
Decisions section — "one abstract behavioral suite... highest-value suite:
guarantees both stores honor one contract"). Written and proven now against
`InMemoryIdempotencyStore`; published so `idempotency-store-redis` and
`idempotency-store-postgres` can extend it later (each supplying their own
Testcontainers-backed instance) without re-deriving the contract per store.

The suite covers the SPI's observable behavior, store-agnostically:
- `reserve` returns *reserved* on a fresh key, or the *existing* record on a
  repeat, with a fence token on the reserved branch.
- A completed record's response is retrievable via `find` and is what a repeat
  reservation attempt returns.
- `release` frees the key for a fresh reservation.
- The fingerprint captured at `reserve` time is retained through `complete`.
- A record past its TTL (lock-ttl for in-progress, response-ttl for completed)
  is treated as absent by `reserve`/`find`.
- `complete`/`release` are no-ops when the fence token no longer matches the
  current reservation (superseded by a fresh one after expiry).

Since this suite must be reusable from other modules' test sources, `core` needs
to publish it as a test artifact (a `test-jar`, or an equivalent mechanism) rather
than keeping it as a private test class.

## Acceptance criteria

- [x] An abstract/parameterized test class defines the contract above, independent
      of any concrete `IdempotencyStore` implementation.
- [x] The suite passes when run against `InMemoryIdempotencyStore`.
- [x] `idempotency-core` publishes this suite as a dependency other modules' test
      sources can extend (verify by confirming the artifact is resolvable, even
      before Redis/Postgres actually consume it).
- [x] Existing `InMemoryIdempotencyStore`-specific tests are not duplicated by the
      new suite (either superseded or left covering something the shared suite
      doesn't, e.g. the store-unavailable simulation).

## Blocked by

None - can start immediately.
