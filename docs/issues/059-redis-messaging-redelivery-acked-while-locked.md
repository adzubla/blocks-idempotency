# Slice 059 — Redis + messaging: the primary's own redelivery is ack-and-skipped while its stale lock lives

> Source: system analysis (2026-11-19) · Type: AFK
> Status: needs-triage

## What to build

ADR 0005 accepts that a concurrent duplicate delivery is ack-and-skipped
(`AbstractMessagingIdempotencyAdvice.handle`, the `Reject` + `IN_PROGRESS`
branch returns `null`), betting that the broker will redeliver the
**primary's own** message natively if it fails. With the Redis store that bet
can lose: the redelivered copy hits the primary's still-live `IN_PROGRESS`
record (same key, same fingerprint), is classified as a concurrent duplicate,
and is acked without the listener ever running. **The message is lost.**

Two ways to get there:

1. **Consumer process crash.** The Redis reservation survives for `lock-ttl`
   (30s default). RabbitMQ (and most JMS brokers) requeue unacked messages as
   soon as the connection drops, so the redelivery lands well inside that
   window. Kafka is usually spared only because the default
   `session.timeout.ms` (45s) exceeds `lock-ttl`.
2. **Store blip during release (no crash needed).** The listener throws,
   `IdempotencyEngine.release` hits `StoreUnavailableException` and swallows
   it, so the key stays `IN_PROGRESS`. Kafka's `DefaultErrorHandler` retries
   immediately (and RabbitMQ/JMS redeliver promptly), so every retry inside
   `lock-ttl` is ack-and-skipped. The message is lost after the error handler's
   retries are used up.

Postgres is not affected: crash = connection close = rollback, and release is a
rollback of the same transaction.

Candidate fixes, for triage:
- For messaging, treat `IN_PROGRESS` as "not yet safe to ack": nack/throw for
  broker redelivery with backoff instead of acking, so the message survives
  until the key resolves one way or the other (re-opens part of ADR 0005's
  trade-off; may warrant an ADR amendment).
- At minimum, don't swallow a failed `release()` in the messaging path: rethrow
  so the delivery isn't acked as a success.
- Document the Redis + RabbitMQ/JMS crash window prominently.

## Acceptance criteria

- [ ] A disabled repro exists for the release-blip path (Redis down during
      `release`, then an immediate redelivery is acked without invoking the listener).
- [ ] Triage decision recorded (nack-on-in-progress, rethrow-on-release-failure,
      document-only), with ADR 0005 amended if its trade-off changes.
- [ ] Whatever is chosen, a redelivery after a failed primary is never silently
      acked while the stale lock is live, or the limitation is documented in the README.

## Blocked by

—
