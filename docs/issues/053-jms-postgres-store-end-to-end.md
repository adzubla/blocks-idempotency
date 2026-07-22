# Slice 053 — Postgres store wired into the JMS module end-to-end

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The exactly-once guarantee proven for a synchronous `@KafkaListener`/
`@RabbitListener` re-verified for a synchronous `@JmsListener`, using
Testcontainers against the shared `route`/`handler` schema (core slice 034):
`reserve()` opens the transaction the listener's effect runs in
(thread-bound, same-thread join, via ordinary `@Transactional`/plain JDBC),
`complete()`/`release()` finish it. Confirms the PRD §7 claim holds for
JMS's own synchronous, single-session-thread listener-container model too.

## Acceptance criteria

- [ ] Happy-path dedupe-skip works end-to-end against a real Postgres
      instance using the shared `route`/`handler` schema
- [ ] A listener's own database write (via `@Transactional`/plain JDBC on
      the same thread) commits together with the idempotency record on
      success, and rolls back together on failure — same guarantee as HTTP,
      Kafka, and RabbitMQ
- [ ] Native concurrency (a second delivery blocks on the row's conflict
      rather than polling) is exercised
- [ ] Covered by an integration test analogous to the Kafka/RabbitMQ
      modules' Postgres suites

## Blocked by

049
