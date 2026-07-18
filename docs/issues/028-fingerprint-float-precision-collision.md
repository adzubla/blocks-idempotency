# Slice 028 — Fingerprint collapses distinct float payloads to one fingerprint

> Source: bug hunt (2026-07-17) · Type: AFK
> Status: needs-triage

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

This slice **exposes and documents** the defect with a disabled repro test; the
fix is deferred (filed unfixed by request). A fix would likely bind numbers
losslessly during canonicalization (e.g. `USE_BIG_DECIMAL_FOR_FLOATS` /
`USE_BIG_INTEGER_FOR_INTS`, or hashing the raw normalized bytes without a
number round-trip), with attention to the flip side — `{"n":1}` vs `{"n":1.0}`
currently produce *different* fingerprints (a false collision / 422 on a
legitimate retry).

## Acceptance criteria

- [x] A disabled repro exists:
      `FingerprintTest.distinctFloatPayloadsRoundingToTheSameDoubleMustNotShareAFingerprint`
      (`@Disabled`, pointing here). Removing `@Disabled` fails today because both
      bodies hash identically.
- [ ] Two payloads that differ only in a float beyond `double` precision produce
      **different** fingerprints.
- [ ] Number canonicalization is decided consistently for the reverse case
      (`1` vs `1.0`, integer vs float), documented, and covered by a test.
- [ ] Existing `FingerprintTest` cases (key reordering, empty body) still pass.

## Blocked by

—

## Comments

- Filed from a code read of the shipped library (bug hunt, 2026-07-17), not a
  friction report. Filed **unfixed by request**: expose now, fix later.
- Impact is a correctness/safety issue, not cosmetic — a different request can
  receive another request's cached response.
