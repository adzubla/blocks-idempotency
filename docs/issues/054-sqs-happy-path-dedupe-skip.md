# Slice 054 — SQS happy-path dedupe-skip (foundation)

> Source: docs/prd/messaging-extension.md (extends the PRD's JMS/RabbitMQ/Kafka scope to a fourth transport — see Batch 8 note in INDEX.md) · Type: AFK
> Status: ready-for-agent

## What to build

The end-to-end spine of an AWS SQS messaging extension, built directly
against `blocks-idempotency-messaging-core` (slice 048) the same way JMS
(slice 049) was — no from-scratch broker adapter, no code sharing left to
a later extraction. New `blocks-idempotency-messaging-sqs` module, on
**Spring Cloud AWS 4.1.0** (`io.awspring.cloud:spring-cloud-aws-starter-sqs`),
not the raw AWS SDK v2 — `@SqsListener` is the annotation-driven listener
seam the shared AOP advice needs, and it's what an application already
consuming SQS via Spring Cloud AWS would already be using (a raw-SDK
polling loop has no annotated method for the advice to intercept, so it
would be architecturally incompatible with the rest of this library, and
with any Spring Cloud AWS application it's meant to drop into).

A `@SqsListener` method annotated `@Idempotent(header=...)` is intercepted
end-to-end via a Spring AOP `@Around` advice built on the shared advice
skeleton from `messaging-core`. Header-strategy key resolution reads from
the inbound `Message<?>`'s `MessageHeaders` (Spring Cloud AWS maps SQS
`MessageAttributes` into headers); body-field strategy reuses
`BodyFieldKeyStrategy` unchanged. The effective key is scoped by
destination (queue name/URL) + listener id + key value — `principal` is
always `NO_PRINCIPAL`. First delivery executes the listener and completes
the record with `CachedResponse.empty()`. A repeat delivery with the same
key is acknowledged (so it isn't redelivered) and the listener is **not**
re-invoked.

Listener methods must run in manual-ack mode (`Acknowledgement`/`Visibility`
parameter, per Spring Cloud AWS's ack-handling model) rather than the
default on-success auto-ack — later slices (collision → dead-letter,
store-failure posture) need to distinguish "acked" from "left for
redelivery/redrive," which requires that control from the start rather
than retrofitting it. Wire that ack mode now even though this slice only
exercises the two happy-path outcomes.

Proven against an in-memory `IdempotencyStore` — real Redis/Postgres wiring
and the action-mapping edge cases (collision, concurrent-dup, store-failure,
missing/invalid key) are follow-on slices, mirroring how the JMS/RabbitMQ
batches were sequenced.

## Acceptance criteria

- [ ] `@Idempotent(header=...)` on a `@SqsListener` method is intercepted
      end-to-end via the shared AOP advice skeleton from `messaging-core`
- [ ] First delivery executes the listener and completes the record
- [ ] A repeat delivery with the same key is acked and the listener is
      **not** re-invoked (the effect runs exactly once)
- [ ] Effective key is scoped by destination (queue name/URL) + listener id
      + key value, with `principal` = `NO_PRINCIPAL`, via `messaging-core`'s
      shared key-factory delegation pattern
- [ ] Listener runs in manual-ack mode (`Acknowledgement`/`Visibility`),
      not Spring Cloud AWS's default auto-ack
- [ ] Covered by a LocalStack (Testcontainers) end-to-end test, plus unit
      tests for the AOP advice and key resolution

## Blocked by

048
