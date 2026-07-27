# Slice 047 — Postgres store wired into the RabbitMQ module end-to-end

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The exactly-once guarantee proven for a synchronous `@KafkaListener` (Kafka
module's slice 042) re-verified for a synchronous `@RabbitListener`, using
Testcontainers against the `route`/`handler` schema (core slice 034):
`reserve()` opens the transaction the listener's effect runs in
(thread-bound, same-thread join, via ordinary `@Transactional`/plain JDBC),
`complete()`/`release()` finish it. Confirms the PRD §7 claim that a
standard synchronous, single-container-thread listener satisfies the same
thread-bound assumption a synchronous MVC controller does, for RabbitMQ's
own listener-container threading model too.

## Acceptance criteria

- [x] Happy-path dedupe-skip works end-to-end against a real Postgres
      instance using the shared `route`/`handler` schema
- [x] A listener's own database write (via `@Transactional`/plain JDBC on
      the same thread) commits together with the idempotency record on
      success, and rolls back together on failure — same guarantee as HTTP
      and Kafka
- [x] Native concurrency (a second delivery blocks on the row's conflict
      rather than polling) is exercised
- [x] Covered by an integration test analogous to the Kafka module's
      Postgres suite (slice 042)

## Blocked by

043
