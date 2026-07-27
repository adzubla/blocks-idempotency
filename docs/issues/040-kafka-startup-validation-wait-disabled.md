# Slice 040 — Kafka startup validation: reject whenInProgress=WAIT

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

A `KafkaListenerEndpointRegistry`-driven startup scan for `@Idempotent` +
`@KafkaListener` methods, mirroring `IdempotentHandlerValidator`'s HTTP
checks (no shared helper — validated directly here) plus one Kafka-specific
rule from `docs/adr/0005-messaging-wait-disabled.md`:

- **Reject `whenInProgress=WAIT`** (explicit or inherited from the global
  default) on any `@KafkaListener`+`@Idempotent` method, failing application
  startup with a clear message. Only `REJECT` is supported for v1 — blocking
  a Kafka listener container thread risks missing `max.poll.interval.ms` and
  triggering a partition rebalance.
- The same checks HTTP's validator enforces: exactly one of
  `header`/`fieldPath` set, a parseable `ttl` when set, and a `store`
  qualifier that resolves to a registered `IdempotencyStore` bean.

## Acceptance criteria

- [x] Startup fails with a clear error if a `@KafkaListener`+`@Idempotent`
      method has `whenInProgress=WAIT` (explicit or inherited from the
      global default)
- [x] Startup fails with a clear error for: both/neither of
      `header`/`fieldPath` set, an invalid `ttl`, or an unresolvable `store`
      qualifier
- [x] A correctly configured `@KafkaListener`+`@Idempotent` method
      (`whenInProgress=REJECT` or unset) passes validation
- [x] Covered by tests for each failure case and the pass case

## Blocked by

035
