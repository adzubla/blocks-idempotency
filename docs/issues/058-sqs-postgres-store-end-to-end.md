# Slice 058 — Postgres store wired into the SQS module end-to-end

> Source: docs/prd/messaging-extension.md (extends the PRD's JMS/RabbitMQ/Kafka scope to SQS — see Batch 8 note in INDEX.md) · Type: AFK
> Status: ready-for-agent

## What to build

The exactly-once guarantee proven for a synchronous `@KafkaListener`/
`@RabbitListener`/`@JmsListener` re-verified for a synchronous
`@SqsListener`, using Testcontainers against the shared `route`/`handler`
schema (core slice 034): `reserve()` opens the transaction the listener's
effect runs in (thread-bound, same-thread join, via ordinary
`@Transactional`/plain JDBC), `complete()`/`release()` finish it. Confirms
the PRD §7 claim holds for Spring Cloud AWS's own synchronous,
single-container-thread listener model too.

## Acceptance criteria

- [ ] Happy-path dedupe-skip works end-to-end against a real Postgres
      instance using the shared `route`/`handler` schema
- [ ] A listener's own database write (via `@Transactional`/plain JDBC on
      the same thread) commits together with the idempotency record on
      success, and rolls back together on failure — same guarantee as HTTP,
      Kafka, RabbitMQ, and JMS
- [ ] Native concurrency (a second delivery blocks on the row's conflict
      rather than polling) is exercised
- [ ] Covered by an integration test analogous to the Kafka/RabbitMQ/JMS
      modules' Postgres suites

## Blocked by

054
