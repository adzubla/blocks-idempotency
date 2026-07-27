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

- [x] Happy-path dedupe-skip works end-to-end against a real Postgres
      instance using the renamed schema
- [x] A listener's own database write (via `@Transactional`/plain JDBC on
      the same thread) commits together with the idempotency record on
      success, and rolls back together on failure — same guarantee as HTTP
- [x] Native concurrency (a second delivery blocks on the row's conflict
      rather than polling) is exercised
- [x] Covered by an integration test analogous to the HTTP Postgres store
      suite

## Findings

- `idempotency_record.handler` was `VARCHAR(10)` — sized for HTTP methods
  (`GET`/`POST`/...) before Slice 034 generalized `handler` to mean "which
  code processes it" generically (HTTP method *or* Kafka listener id). Any
  listener id longer than 10 characters (e.g. `orders-listener`) failed
  every reservation with a Postgres "value too long for type character
  varying(10)" error — not a latent bug, this slice's happy-path test hit
  it immediately. Widened to `VARCHAR(255)` (`V1__idempotency_record.sql`,
  edited in place per Slice 034's note that the library is unreleased).

## Blocked by

035, 034
