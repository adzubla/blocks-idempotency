# Slice 056 — SQS startup validation: reject `whenInProgress=WAIT`

> Source: docs/prd/messaging-extension.md (extends the PRD's JMS/RabbitMQ/Kafka scope to SQS — see Batch 8 note in INDEX.md) · Type: AFK
> Status: ready-for-agent

## What to build

A startup-validation scan for `@Idempotent` + `@SqsListener` methods,
mirroring `IdempotentHandlerValidator`'s HTTP checks and the Kafka (040),
RabbitMQ (045), and JMS (051) modules' own validators: key-strategy/ttl/store
checks, plus the WAIT-rejection rule from
`docs/adr/0005-messaging-wait-disabled.md`. A `@SqsListener` method
annotated `@Idempotent(whenInProgress=WAIT)` fails application startup with
a clear error, for the same reasons documented in ADR 0005 — blocking a
Spring Cloud AWS listener container thread inside `store.await()` risks the
container's own concurrency/visibility-timeout bookkeeping treating the
poller as stalled, a worse failure mode than the narrow correctness
fallback WAIT provides, and is redundant given SQS's own
redelivery/visibility-timeout mechanics.

## Acceptance criteria

- [ ] A `@SqsListener` + `@Idempotent(whenInProgress=WAIT)` method fails
      application startup with a clear error message
- [ ] Existing key-strategy/ttl/store startup checks (mirroring
      `IdempotentHandlerValidator`) are ported to the SQS listener scan
- [ ] `whenInProgress=REJECT` continues to start up cleanly
- [ ] Covered by a startup-validation test analogous to the
      Kafka/RabbitMQ/JMS modules'

## Blocked by

054
