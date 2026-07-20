# Slice 042 — Postgres store wired into the Kafka module end-to-end

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The exactly-once guarantee proven for a synchronous `@KafkaListener`, using
Testcontainers against the renamed `route`/`handler` schema (slice 034):
`reserve()` opens the transaction the listener's effect runs in
(thread-bound, same-thread join, via ordinary `@Transactional`/plain JDBC),
`complete()`/`release()` finish it — mirroring what HTTP slices 017–019
proved, now driven by a Kafka listener instead of an MVC handler. Confirms
the PRD §7 claim that a standard synchronous, single-container-thread
`@KafkaListener` satisfies the same thread-bound assumption a synchronous
MVC controller does.

## Acceptance criteria

- [ ] Happy-path dedupe-skip works end-to-end against a real Postgres
      instance using the renamed schema
- [ ] A listener's own database write (via `@Transactional`/plain JDBC on
      the same thread) commits together with the idempotency record on
      success, and rolls back together on failure — same guarantee as HTTP
- [ ] Native concurrency (a second delivery blocks on the row's conflict
      rather than polling) is exercised
- [ ] Covered by an integration test analogous to the HTTP Postgres store
      suite

## Blocked by

035, 034
