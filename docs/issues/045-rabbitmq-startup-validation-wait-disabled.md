# Slice 045 — RabbitMQ startup validation: reject `whenInProgress=WAIT`

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

A startup-validation scan for `@Idempotent` + `@RabbitListener` methods,
mirroring `IdempotentHandlerValidator`'s HTTP checks and the Kafka module's
own validator (slice 040): key-strategy/ttl/store checks, plus the
WAIT-rejection rule from `docs/adr/0005-messaging-wait-disabled.md`. A
`@RabbitListener` method annotated `@Idempotent(whenInProgress=WAIT)` fails
application startup with a clear error, for the same reasons documented in
ADR 0005 — blocking a RabbitMQ listener container thread inside
`store.await()` risks the container treating the consumer as stalled, a
worse failure mode than the narrow correctness fallback WAIT provides, and
is redundant given RabbitMQ's own broker-native redelivery.

## Acceptance criteria

- [ ] A `@RabbitListener` + `@Idempotent(whenInProgress=WAIT)` method fails
      application startup with a clear error message
- [ ] Existing key-strategy/ttl/store startup checks (mirroring
      `IdempotentHandlerValidator`) are ported to the RabbitMQ listener scan
- [ ] `whenInProgress=REJECT` continues to start up cleanly
- [ ] Covered by a startup-validation test analogous to the Kafka module's

## Blocked by

043
