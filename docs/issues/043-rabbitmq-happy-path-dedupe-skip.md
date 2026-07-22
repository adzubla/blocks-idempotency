# Slice 043 — RabbitMQ happy-path dedupe-skip (foundation)

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The end-to-end spine of the RabbitMQ messaging extension, mirroring how the
Kafka module's slice 035 established its foundation. New
`blocks-idempotency-messaging-rabbitmq` module.

A `@RabbitListener` method annotated `@Idempotent(header=...)` is intercepted
end-to-end via a Spring AOP `@Around` advice (the same mechanism the Kafka
module uses, and the same one `@Transactional` already uses on listener
methods — no buffered-request wrapping needed, the message payload is
already a plain method argument). Header-strategy key resolution reads from
the AMQP message's headers (`MessageProperties.getHeaders()`); body-field
strategy reuses `BodyFieldKeyStrategy` unchanged. The effective key is scoped
by destination (queue) + listener id + key value — `principal` is always
`NO_PRINCIPAL` (no servlet-principal equivalent for v1, per the PRD). First
delivery executes the listener and completes the record with
`CachedResponse.empty()`. A repeat delivery with the same key is
acknowledged and the listener is **not** re-invoked.

Proven against an in-memory `IdempotencyStore` — real Redis/Postgres wiring
is slices 046/047. Reuse whatever is already transport-neutral in `core`
(`EffectiveKeyFactory`, `IdempotencyEngine`, `BodyFieldKeyStrategy`) rather
than re-deriving it; don't yet try to share code with the Kafka module — that
extraction is slice 048, once RabbitMQ gives it a second real data point.

## Acceptance criteria

- [ ] `@Idempotent(header=...)` on a `@RabbitListener` method is intercepted
      end-to-end via AOP advice
- [ ] First delivery executes the listener and completes the record
- [ ] A repeat delivery with the same key is acked and the listener is
      **not** re-invoked (the effect runs exactly once)
- [ ] Effective key is scoped by destination (queue) + listener id + key
      value, with `principal` = `NO_PRINCIPAL`
- [ ] Covered by an embedded/test-broker (or equivalent) end-to-end test,
      plus unit tests for the AOP advice and key resolution

## Blocked by

None - can start immediately.
