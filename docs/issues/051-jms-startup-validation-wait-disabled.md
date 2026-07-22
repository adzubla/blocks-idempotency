# Slice 051 — JMS startup validation: reject `whenInProgress=WAIT`

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

A startup-validation scan for `@Idempotent` + `@JmsListener` methods, reusing
`messaging-core`'s shared validation-rule shape (slice 048: key-strategy/ttl/
store checks + the WAIT-rejection rule from
`docs/adr/0005-messaging-wait-disabled.md`), with JMS's own listener-endpoint
scanning mechanism underneath it. A `@JmsListener` method annotated
`@Idempotent(whenInProgress=WAIT)` fails application startup with a clear
error, for the same reasons documented in ADR 0005 — blocking a JMS listener
thread inside `store.await()` risks similar container-level disruption to a
Kafka rebalance or a stalled RabbitMQ consumer, and is redundant given JMS's
own broker-native redelivery.

## Acceptance criteria

- [ ] A `@JmsListener` + `@Idempotent(whenInProgress=WAIT)` method fails
      application startup with a clear error message
- [ ] Key-strategy/ttl/store startup checks are ported to the JMS listener
      scan via `messaging-core`'s shared rule shape
- [ ] `whenInProgress=REJECT` continues to start up cleanly
- [ ] Covered by a startup-validation test analogous to the Kafka/RabbitMQ
      modules'

## Blocked by

049
