# Slice 027 — Move the Postgres test-only escape hatch out of the production class

> Source: code-review smell scan (2026-07-16) · Type: AFK
> Status: wontfix

## What to build

`PostgresIdempotencyStore.java` mixes several unrelated reasons to change:
transaction/reservation logic, native WAIT blocking, JSON header
(de)serialization, connection-failure classification, and a test-only escape
hatch (`abandonDanglingReservationForTests()`, line 367). That last method is
a reason to touch this production file for test-support needs only. Move it
behind a package-private test seam (e.g. a test-tree subclass or a
`@VisibleForTesting`-style helper injected only from the test suite) so the
production class no longer carries test-only surface area.

## Acceptance criteria

- [ ] `abandonDanglingReservationForTests()` (or its replacement) is no
      longer a method on the production `PostgresIdempotencyStore` class
      reachable from production code paths.
- [ ] The contract/behavior it enables for tests (abandoning a dangling
      reservation to simulate a crashed primary) remains exercisable by the
      existing test suite, via the new seam.
- [ ] All existing Postgres store tests continue to pass unmodified in
      behavior (only the seam mechanics change).

## Blocked by

—

## Comments

- Filed from a code-smell review of the codebase (Divergent Change).
