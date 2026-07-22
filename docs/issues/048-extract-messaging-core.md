# Slice 048 — Extract `blocks-idempotency-messaging-core`

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

A wide-refactor slice, not a new-behavior one: with both
`blocks-idempotency-messaging-kafka` (slices 035–042) and
`blocks-idempotency-messaging-rabbitmq` (slices 043–047) complete, draw the
actual broker-agnostic boundary from two real data points instead of
guessing it up front, per the PRD's Phase 3 note. Extract whatever proves
genuinely shared into a new `blocks-idempotency-messaging-core` module;
leave whatever is genuinely broker-specific (header/property reading,
dead-letter/nack mechanics, listener-endpoint scanning for startup
validation) in each broker module.

Expected candidates for extraction, to confirm or revise once both modules
are side by side:

- The action-mapping translation from `EngineDecision`/`RejectReason` to the
  broker-neutral verbs (ack-and-skip / dead-letter / nack-with-backoff),
  currently duplicated per broker.
- The listener-scoped `EffectiveKeyFactory` delegation pattern (destination +
  listener-id + key value, `principal=NO_PRINCIPAL`) each broker's key
  factory currently repeats.
- The shared shape of the startup-validation rules (key-strategy/ttl/store
  checks + the WAIT-rejection rule from ADR 0005) apart from each broker's
  own endpoint-scanning mechanism.
- The AOP `@Around` advice skeleton (before/complete/release call sequence
  against the engine), parameterized by a broker-specific key-resolution and
  action-execution seam.

This is expand-migrate-contract in spirit but scoped small enough to land as
one slice: move the shared code into the new module, repoint both existing
modules' `pom.xml`s to depend on it, delete the duplicated copies. No
listener-visible behavior changes in either broker module.

## Acceptance criteria

- [ ] New `blocks-idempotency-messaging-core` module exists, depended on by
      both `blocks-idempotency-messaging-kafka` and
      `blocks-idempotency-messaging-rabbitmq`
- [ ] No duplicated action-mapping/key-factory-delegation/validation-rule
      logic remains between the two broker modules
- [ ] Full existing test suites for both broker modules pass unchanged —
      pure extraction, no behavior change: `mvn test -pl
      blocks-idempotency-messaging-kafka -am`, `mvn test -pl
      blocks-idempotency-messaging-rabbitmq -am`
- [ ] `blocks-idempotency-messaging-core`'s own extracted pieces are unit
      tested directly, not just transitively through the broker modules

## Blocked by

043, 044, 045, 046, 047
