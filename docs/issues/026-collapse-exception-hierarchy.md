# Slice 026 — Collapse the `IdempotencyException` subclass hierarchy

> Source: code-review smell scan (2026-07-16) · Type: AFK
> Status: wontfix

## What to build

Five subclasses of `IdempotencyException` — `IdempotencyCollisionException`,
`IdempotencyFailClosedException`, `IdempotencyKeyInvalidException`,
`IdempotencyKeyRequiredException`, `IdempotencyResponseUnavailableException` —
each have a body that is only `super(fixedStatus, fixedReason, fixedMessage)`;
none add behavior (Middle Man). This also causes Shotgun Surgery: adding a new
terminal outcome currently means touching `RejectReason.java`, a new exception
subclass, the exhaustive switch in
`IdempotencyInterceptor.java:109-133`, and the `instanceof` check in
`IdempotencyExceptionHandler.java:37`.

Collapse to a single `IdempotencyException` with static factory methods keyed
by `RejectReason` (or generate the status/reason/message mapping from the
enum), so a new outcome requires editing `RejectReason` and one mapping site
instead of four.

## Acceptance criteria

- [ ] The five boilerplate subclasses are removed; `IdempotencyException`
      gains static factories (or equivalent) covering the same cases.
- [ ] `IdempotencyInterceptor`'s switch and `IdempotencyExceptionHandler`'s
      `instanceof` check are replaced with a single dispatch on
      `RejectReason` (or the exception's carried reason), removing at least
      two of the four current edit sites for a new outcome.
- [ ] HTTP status codes, reject reasons, and messages returned to clients are
      byte-for-byte unchanged for all existing cases (this is a pure
      refactor, not a behavior change) — verified by existing web/interceptor
      tests passing unmodified.

## Blocked by

—

## Comments

- Filed from a code-smell review of the codebase (Middle Man, and the
  Shotgun Surgery it causes).
- Declined (2026-07-17): the five subclasses aren't just internal plumbing -
  they're a documented public extension point. `README.md:262-284` lists all
  five by name in a table and shows `@ExceptionHandler(IdempotencyCollisionException.class)`
  as the supported way to override one specific outcome; collapsing them
  removes that per-type catch surface and forces consumers onto a runtime
  `ex.reason()` check instead. The acceptance criteria only promised
  wire-level behavior (status/headers/body) was unchanged and didn't account
  for that Java-API-shape change, so "pure refactor" undersold what was
  actually being proposed. Separately, the duplication being removed is thin
  (each subclass is one `super(status, reason, message)` line - a static
  factory table or a `RejectReason -> (status, message)` map isn't obviously
  less code, just relocated), a new outcome already requires touching
  `RejectReason` regardless of which design wins, and the sealed `permits`
  clause currently buys a compiler error if a new subtype isn't wired up
  everywhere - a safety net that weakens once everything funnels through
  generic factories. No real pain point drove this (smell scan, not a bug or
  friction report), so the marginal win doesn't justify touching a documented
  public contract.
