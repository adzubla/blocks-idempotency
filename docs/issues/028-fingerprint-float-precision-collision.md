# Slice 028 — Fingerprint collapses distinct float payloads to one fingerprint

> Source: bug hunt (2026-07-17) · Type: AFK

## What to build

`Fingerprint.normalize` (`fingerprint/Fingerprint.java:58-67`) parses the
request body as `Object.class` and re-serializes it to canonical JSON. Jackson
binds JSON **floating-point** numbers to `double`, so two genuinely different
payloads whose numbers round to the same IEEE-754 double produce byte-identical
canonical JSON and therefore the **same** SHA-256 fingerprint.

The engine's collision guard (`engine/IdempotencyEngine.java:79`,
`existing.fingerprint().equals(fingerprint)`) then sees a match, so a *different*
payload sent under the same idempotency key is treated as a duplicate and
**replays the first caller's cached 2xx response instead of returning 422** —
directly defeating the library's stated purpose ("detect a key reused with a
different payload", `Fingerprint.java:12-15`).

Reproduced empirically: `9007199254740992.0` and `9007199254740993.0` are
distinct decimal literals that both parse to the same double; likewise
`1.0` and `1.000000000000000001`.

**Fixed** by binding floating-point numbers to exact `BigDecimal` during
canonicalization (`DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS`) instead
of lossy `double`; integral numbers are already lossless (`long`/`BigInteger`).
Numbers are thus compared by their exact JSON text, so the flip side —
`{"n":1}` vs `{"n":1.0}` — deliberately stays *distinct* (a conservative choice:
a differing byte payload is treated as a collision, never silently merged).

## Acceptance criteria

- [x] A regression test exists:
      `FingerprintTest.distinctFloatPayloadsRoundingToTheSameDoubleMustNotShareAFingerprint`
      (enabled, passing).
- [x] Two payloads that differ only in a float beyond `double` precision produce
      **different** fingerprints.
- [x] Number canonicalization is decided consistently for the reverse case
      (`1` vs `1.0`, integer vs float) — kept *distinct*, pinned by
      `FingerprintTest.integerAndFloatSpellingsOfTheSameValueProduceDifferentFingerprints`.
- [x] Existing `FingerprintTest` cases (key reordering, empty body) still pass.

## Blocked by

—

## Comments

- Filed from a code read of the shipped library (bug hunt, 2026-07-17), not a
  friction report.
- Impact was a correctness/safety issue, not cosmetic — a different request
  could receive another request's cached response.
- Implemented (2026-07-17): `Fingerprint.CANONICAL_MAPPER` now enables
  `USE_BIG_DECIMAL_FOR_FLOATS` so floats are read as exact decimals rather than
  rounded to `double`. Repro test un-`@Disabled` and now passes; a second test
  pins the `1` vs `1.0` decision; full `FingerprintTest` and the core suite pass
  (`mvn test -pl blocks-idempotency-core -am`).
