 # PRD — Extending Idempotency to Messaging (JMS/RabbitMQ/Kafka)

> Source: feasibility/design analysis of extending `blocks-idempotency-core`
> (currently HTTP/Spring-MVC only) to message-consumer idempotency
> (`@KafkaListener`/`@RabbitListener`/`@JmsListener` deduplication). Grounded in
> direct inspection of `blocks-idempotency-core`, `-store-redis`, and
> `-store-postgres` as of 2026-07-20. No corresponding discussion exists yet in
> `CONTEXT.md` or `docs/adr/*.md` — this is new ground, not filling a
> documented gap. Not yet committed for implementation.

## Problem Statement

The library today only protects synchronous HTTP request/response handlers.
Teams using Spring for Kafka/RabbitMQ/JMS hit the identical class of bug this
library already solves for REST — at-least-once redelivery causes a listener
to process the same message twice (a payment charged twice, an order created
twice) — but have no equivalent drop-in guard for `@KafkaListener`/
`@RabbitListener`/`@JmsListener` methods. They'd have to reinvent the same
ad-hoc unique-constraint/check-before-insert pattern the HTTP library was
built to replace.

## Verdict

**Feasible, and it's an extension, not a rewrite.** The library already
separates a transport-agnostic core from a thin HTTP adapter layer. The
engine's own javadoc claims "framework-free — depends only on the store and
model abstractions," and that holds up on inspection: none of `engine/`,
`policy/`, or the `IdempotencyStore` contract touch servlet types. Messaging
support means building a new adapter (Spring AOP advice + broker-specific key
extraction) that calls the same `before()`/`complete()`/`release()` engine
methods an HTTP request does today. The stores need no new implementation —
Redis needs no changes at all; Postgres needs schema/field generalization
only.

### Reusable as-is

- `engine/IdempotencyEngine.java`, `engine/EngineDecision.java`,
  `engine/IdempotencyEngineRegistry.java`
- `policy/IdempotencyPolicy.java`, `policy/PolicyResolver.java`
- `store/IdempotencyStore.java` — every method already takes generic types
  (`EffectiveKey`/`CachedResponse`/`Duration`/`String`)
- `key/BodyFieldKeyStrategy.java`, `key/KeyFormat.java`
- `model/RecordState.java`, `model/ReservationResult.java`
- `RedisIdempotencyStore` — keys opaquely off `EffectiveKey.digestBytes()`,
  never inspects fields individually

### HTTP-coupled, needs a message-oriented equivalent

- `web/IdempotencyFilter.java` + `web/CachedBodyHttpServletRequest.java` — not
  needed at all for messaging; a message payload already arrives as a plain
  method argument, nothing to buffer for re-read.
- `web/IdempotencyInterceptor.java` — the `preHandle`/`afterCompletion` pair
  has no listener analog. Becomes a Spring AOP `@Around` advice instead — the
  same mechanism `@Transactional` already uses on listener methods.
- `response/ResponseCapture.java` / `ResponseReplayer.java` — bound to
  `ContentCachingResponseWrapper`/`HttpServletResponse`.
- `key/EffectiveKeyFactory.java` + `key/HeaderKeyStrategy.java` — read off
  `HttpServletRequest`; message headers are the direct equivalent source.
- The 6 `IdempotencyException` subtypes — carry `HttpStatus`, translated via
  `@ControllerAdvice`.
- `config/IdempotencyAutoConfiguration.java` +
  `validation/IdempotentHandlerValidator.java` — Spring-MVC-specific
  (`FilterRegistrationBean`, `WebMvcConfigurer`, `RequestMappingHandlerMapping`).

## Design Changes

### 1. Generalize three model types in `core` (additively)

Three types are HTTP-*named* but not HTTP-*shaped*:

| Type | Current shape | Generalization |
|---|---|---|
| `model/EffectiveKey` | `(method, path, principal, value)` | Neutral field names, e.g. `(scope1, scope2, principal, value)` — the same four dimensions cover `(destination/topic, listener-id, principal, key)` for messaging |
| `fingerprint/Fingerprint.sha256(method, path, body)` | HTTP verb + URI + payload hash | Algorithm (SHA-256, NUL-joined, canonical-JSON body normalization) is already payload-agnostic — just a param rename |
| `model/CachedResponse` | `(status, headers, body)` | Structurally "outcome + metadata + payload" already — reusable for the optional case of republishing a reply message |

Do this **additively**: keep `EffectiveKeyFactory.create(HttpServletRequest,
...)` and `HeaderKeyStrategy.resolve(HttpServletRequest, ...)` as HTTP-specific
factory methods, rename only the underlying record field names, and add
parallel factory methods (e.g. `EffectiveKeyFactory.create(String destination,
String consumerId, String principal, String key)`) for messaging. Since
`EffectiveKey` is normally obtained through factories rather than constructed
ad hoc, this avoids breaking existing call sites.

### 2. New adapter modules, mirroring the store-module split

Mirror the existing `-store-redis`/`-store-postgres` pattern with
`blocks-idempotency-messaging-kafka`, `-rabbit`, `-jms` (factoring out a shared
`-messaging-core` once it's clear what's genuinely common). Shared across all
three brokers:

- **Wrapping mechanism** — all three integrate via Spring-managed beans with
  annotated listener methods, so one Spring AOP `@Around` advice pattern
  replaces `IdempotencyFilter`/`IdempotencyInterceptor` uniformly.
- **Startup validation** — an `IdempotentHandlerValidator`-equivalent scanning
  for `@Idempotent` + `@XListener` methods (mechanism differs per broker:
  `KafkaListenerEndpointRegistry` vs. reflecting Rabbit/JMS listener beans).

Broker-specific: key/header extraction (`ConsumerRecord.headers()` vs. AMQP
`MessageProperties` vs. JMS `Message.getStringProperty`), and ack/nack/dead-letter
semantics, since each broker has a different redelivery model.

### 3. Map `RejectReason` to consumer actions, not HTTP status

`RejectReason` already lives in the framework-agnostic `engine/` package — no
rework needed there, only the translation layer. Instead of
`IdempotencyExceptionHandler` mapping a reason to an HTTP status +
`Idempotency-Reject-Reason` header, a messaging adapter maps it to one of:

- **ack-and-skip** — duplicate, already completed
- **nack-with-backoff/retry** — transient store failure
- **dead-letter** — validation failure (missing/invalid key, or a fingerprint
  collision indicating a poison message)

**Policy nuance**: `whenInProgress=WAIT` blocks the calling thread until the
in-flight duplicate resolves. For an HTTP request thread that's locally
contained; for a listener container thread it risks stalling partition
consumption or triggering a consumer-group rebalance if the wait exceeds
session/poll timeouts. Recommend **REJECT-style behavior (ack-and-skip or
nack-with-backoff) as the default for messaging**, keeping `WAIT` available as
an opt-in for short, bounded processing times.

### 4. Store implications

- **Redis**: no changes needed.
- **Postgres**: schema (`http_method, path, principal, idempotency_key`, PK on
  those four) needs renaming/generalizing — a Flyway migration renaming
  columns is preferable to a parallel table, to avoid duplicating the store
  implementation. The thread-bound-transaction constraint (`reserve()` opens a
  transaction that `complete()`/`release()` must finish on the same thread) is
  **not a new problem** — it already excludes WebFlux/async servlet handlers
  per the store's own javadoc, and a standard synchronous, single-container-thread
  `@KafkaListener`/`@RabbitListener`/`@JmsListener` satisfies the same
  assumption a synchronous MVC controller does today. Document this as a
  carried-over constraint; it would still break under reactive consumers
  (Reactor Kafka, deferred manual-ack to another thread).

## Recommended Phased Build Order

1. **Generalize `core`** — rename `EffectiveKey`/`Fingerprint`/`CachedResponse`
   fields additively; extract a formal key-strategy SPI parallel to today's
   header/body strategies. No new modules yet.
2. **One broker adapter end-to-end as proof of concept** — likely Kafka
   (largest ecosystem overlap, clean `ConsumerRecord.headers()` model),
   including the Postgres migration and validating the AOP advice mechanism
   against real listener-container behavior (rebalance/redelivery edge cases).
3. **Extend the pattern to RabbitMQ and JMS**, factoring out whatever proves
   genuinely broker-agnostic from the Kafka work into a shared messaging-core
   module.

## Critical Files (for whichever phase is picked up next)

- `blocks-idempotency-core/src/main/java/io/adzubla/blocks/idempotency/engine/IdempotencyEngine.java` — the reusable orchestration core
- `blocks-idempotency-core/src/main/java/io/adzubla/blocks/idempotency/model/EffectiveKey.java` — field-naming generalization
- `blocks-idempotency-core/src/main/java/io/adzubla/blocks/idempotency/model/CachedResponse.java` — outcome/payload generalization
- `blocks-idempotency-core/src/main/java/io/adzubla/blocks/idempotency/fingerprint/Fingerprint.java` — param renaming
- `blocks-idempotency-core/src/main/java/io/adzubla/blocks/idempotency/web/IdempotencyInterceptor.java` — reference for the AOP advice this becomes
- `blocks-idempotency-core/src/main/java/io/adzubla/blocks/idempotency/config/IdempotencyAutoConfiguration.java` — reference for the wiring a messaging adapter needs to replace
- `blocks-idempotency-store-postgres/src/main/resources/db/migration/V1__idempotency_record.sql` — schema to generalize

## Verification (once a phase is picked up)

- **Phase 1** (core generalization): run `mvn test -pl blocks-idempotency-core`
  — the shared `IdempotencyStoreContractTest` suite must still pass unchanged
  against `InMemoryIdempotencyStore` after the field rename, proving it was
  additive.
- **Phase 2** (Kafka POC): an embedded-Kafka integration test exercising a
  duplicate delivery (same key, same/different payload) against a real
  `@KafkaListener` + `@Idempotent`, asserting ack-and-skip / dead-letter /
  collision behavior, plus a Postgres integration test confirming the renamed
  schema and the thread-bound transaction join still work end-to-end via
  `mvn test -pl blocks-idempotency-store-postgres -am`.
