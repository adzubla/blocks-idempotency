# Slice 049 — JMS happy-path dedupe-skip (foundation)

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The end-to-end spine of the JMS messaging extension, built directly against
`blocks-idempotency-messaging-core` (slice 048) rather than copying the
Kafka/RabbitMQ pattern from scratch a third time. New
`blocks-idempotency-messaging-jms` module.

A `@JmsListener` method annotated `@Idempotent(header=...)` is intercepted
end-to-end via a Spring AOP `@Around` advice built on the shared advice
skeleton from `messaging-core`. Header-strategy key resolution reads from
the JMS `Message`'s properties (`Message.getStringProperty(...)`); body-field
strategy reuses `BodyFieldKeyStrategy` unchanged. The effective key is scoped
by destination (queue/topic) + listener id + key value — `principal` is
always `NO_PRINCIPAL`. First delivery executes the listener and completes
the record with `CachedResponse.empty()`. A repeat delivery with the same
key is acknowledged and the listener is **not** re-invoked.

Proven against an in-memory `IdempotencyStore` — real Redis/Postgres wiring
is slices 052/053.

## Acceptance criteria

- [ ] `@Idempotent(header=...)` on a `@JmsListener` method is intercepted
      end-to-end via the shared AOP advice skeleton from `messaging-core`
- [ ] First delivery executes the listener and completes the record
- [ ] A repeat delivery with the same key is acked and the listener is
      **not** re-invoked (the effect runs exactly once)
- [ ] Effective key is scoped by destination (queue/topic) + listener id +
      key value, with `principal` = `NO_PRINCIPAL`, via
      `messaging-core`'s shared key-factory delegation pattern
- [ ] Covered by an embedded-broker (e.g. ActiveMQ Artemis in-VM) end-to-end
      test, plus unit tests for the AOP advice and key resolution

## Blocked by

048
