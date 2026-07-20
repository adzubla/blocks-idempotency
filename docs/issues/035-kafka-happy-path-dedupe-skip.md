# Slice 035 — Kafka happy-path dedupe-skip (foundation)

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The end-to-end spine of the Kafka messaging extension, mirroring how HTTP
slice 001 established the foundation. New `blocks-idempotency-messaging-kafka`
module.

A `@KafkaListener` method annotated `@Idempotent(header=...)` is intercepted
end-to-end via a Spring AOP `@Around` advice (the same mechanism
`@Transactional` already uses on listener methods — no buffered-request
wrapping needed, the message payload is already a plain method argument).
Header-strategy key resolution reads from `ConsumerRecord.headers()`;
body-field strategy reuses `BodyFieldKeyStrategy` unchanged. The effective
key is scoped by destination (topic) + listener id + key value — `principal`
is always `NO_PRINCIPAL` (no servlet-principal equivalent for v1, per the
PRD). First delivery executes the listener and completes the record with
`CachedResponse.empty()` (slice 034). A repeat delivery with the same key is
acknowledged and the listener is **not** re-invoked.

Proven against an in-memory `IdempotencyStore` — real Redis/Postgres wiring
is slices 041/042.

## Acceptance criteria

- [ ] `@Idempotent(header=...)` on a `@KafkaListener` method is intercepted
      end-to-end via AOP advice
- [ ] First delivery executes the listener and completes the record
- [ ] A repeat delivery with the same key is acked and the listener is
      **not** re-invoked (the effect runs exactly once)
- [ ] Effective key is scoped by destination (topic) + listener id + key
      value, with `principal` = `NO_PRINCIPAL`
- [ ] Covered by an embedded-Kafka (or equivalent) end-to-end test, plus unit
      tests for the AOP advice and key resolution

## Blocked by

034
