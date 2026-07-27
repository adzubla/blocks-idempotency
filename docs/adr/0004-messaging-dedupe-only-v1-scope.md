# Messaging v1 ships dedupe-only, no response replay

## Status

accepted

## Context and decision

The HTTP idempotency mechanism caches and replays the *original response*
(status/headers/body) on a repeat request — the client gets back exactly
what the first execution produced. Extending idempotency to message
listeners (`docs/prd/messaging-extension.md`) raised the question of whether
a duplicate delivery should carry the same replay behavior — e.g.
republishing whatever was sent to a reply/output topic the first time.

We decided **v1 is dedupe-only**: a duplicate delivery is acknowledged and
skipped. Nothing is re-emitted, re-published, or handed back to anything.
The record's completed state exists purely to answer "has this already run,"
not "what did it produce."

This matches the dominant messaging use case — "don't run the side effect
twice" (don't charge a payment twice, don't create an order twice) — which
doesn't require reconstructing the first execution's output, unlike an HTTP
client that is actively waiting on a response it needs to consume.

Consequence traced through the engine: `IdempotencyEngine.complete(...)`
keeps its exact signature (`CachedResponse response` stays a required
param); messaging call sites always pass an empty/sentinel `CachedResponse`.
Since `IdempotencyEngine.decisionForCompleted()` only returns `Replay` when
`response.hasBody()` is true, every completed messaging duplicate resolves
to `EngineDecision.Unavailable` instead — a decision type that means
"terminal error, don't retry" in HTTP, but is the **ordinary, expected
outcome for a routine duplicate skip** in messaging. This is deliberate
reuse, not a bug: it avoids any change to the engine's public API, at the
cost of a decision label whose meaning differs by transport. See
`docs/prd/messaging-extension.md` §2 for the full mapping.

## Considered Options

- **Full replay, mirroring HTTP** (rejected for v1) — generalize
  `CachedResponse`'s shape to carry an arbitrary captured outcome, and
  republish it to a reply topic/downstream event on a duplicate. Kept as a
  future, broker-specific opt-in rather than the default: it adds real
  complexity (what does "republish" mean per broker — a Kafka `@SendTo`
  reply, an AMQP reply-to queue, a JMS reply destination — with no single
  answer) for a need that doesn't exist yet.

## Consequences

- No `ResponseCapture`/`ResponseReplayer`-equivalent is needed for the
  messaging adapter at all in v1 — not "optional," genuinely unnecessary.
- Revisiting this later to support replay means giving messaging's
  `CachedResponse` real content (so `hasBody()` becomes true for those
  records), which changes the `Unavailable`-vs-`Replay` split described
  above — a deliberate, contained change to make if the need arises, not
  something this decision forecloses.
