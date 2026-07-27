# PRD — Extending Idempotency to Messaging (JMS/RabbitMQ/Kafka)

> Source: feasibility/design analysis of extending `blocks-idempotency-core`
> (currently HTTP/Spring-MVC only) to message-consumer idempotency
> (`@KafkaListener`/`@RabbitListener`/`@JmsListener` deduplication). Grounded in
> direct inspection of `blocks-idempotency-core`, `-store-redis`, and
> `-store-postgres`, and sharpened via a `/grill-with-docs` interview session,
> both as of 2026-07-20. See `docs/adr/0004-messaging-dedupe-only-v1-scope.md`
> and `docs/adr/0005-messaging-wait-disabled.md` for the two decisions judged
> hard-to-reverse/surprising enough to warrant a standalone ADR. Not yet
> committed for implementation.

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

**v1 scope is deliberately narrow**: dedupe-only (skip a duplicate delivery,
nothing re-emitted), Kafka only, single module, `WAIT` concurrency mode
disabled. See ADRs 0004/0005 for why.

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
- `IdempotencyEngine.complete(...)`'s signature, unchanged — see "Engine
  stays unchanged" below

### HTTP-coupled, needs a message-oriented equivalent

- `web/IdempotencyFilter.java` + `web/CachedBodyHttpServletRequest.java` — not
  needed at all for messaging; a message payload already arrives as a plain
  method argument, nothing to buffer for re-read.
- `web/IdempotencyInterceptor.java` — the `preHandle`/`afterCompletion` pair
  has no listener analog. Becomes a Spring AOP `@Around` advice instead — the
  same mechanism `@Transactional` already uses on listener methods.
- `response/ResponseCapture.java` / `ResponseReplayer.java` — bound to
  `ContentCachingResponseWrapper`/`HttpServletResponse`. **Not needed at all
  for v1**: since v1 is dedupe-only (no replay), there's nothing to capture
  or replay — see "Dedupe-only v1 scope" below.
- `web/key/HttpEffectiveKeyFactory.java` + `web/key/HeaderKeyStrategy.java`
  (in `blocks-idempotency-web`, per ADR 0006) — read off `HttpServletRequest`;
  message headers are the direct equivalent source (Kafka
  `ConsumerRecord.headers()`). `key/EffectiveKeyFactory.java` itself (in
  `blocks-idempotency-core`) is **already** the transport-neutral
  construction seam a messaging adapter uses — see §3.
- The 6 `IdempotencyException` subtypes — carry `HttpStatus`, translated via
  `@ControllerAdvice`. Messaging replaces this with the action mapping below.
- `web/config/IdempotencyWebAutoConfiguration.java` +
  `validation/IdempotentHandlerValidator.java` (in `blocks-idempotency-web`)
  — Spring-MVC-specific (`FilterRegistrationBean`, `WebMvcConfigurer`,
  `RequestMappingHandlerMapping`). The messaging equivalent also needs one
  extra rule the HTTP validator doesn't have: rejecting `whenInProgress=WAIT`
  on a listener method at startup (ADR 0005).

## Design Changes

### 1. Dedupe-only v1 scope (no response replay)

A duplicate delivery is acked and skipped; nothing is republished to a
reply/output topic. See `docs/adr/0004-messaging-dedupe-only-v1-scope.md`
for the full reasoning. This is a scope decision, not a limitation of the
core — a later broker-specific extension can opt into real replay by giving
`CachedResponse` genuine content, without touching the engine.

### 2. Engine stays unchanged — the sentinel `CachedResponse`, and what it implies

`IdempotencyEngine.complete(key, fenceToken, CachedResponse response, ttl)`
keeps its exact signature. Messaging call sites always complete with an
empty/sentinel `CachedResponse` (it already models "no body" via
`hasBody()`).

This has a non-obvious, broker-agnostic consequence worth documenting
explicitly so it isn't "rediscovered" as a bug later:
`IdempotencyEngine.decisionForCompleted()` only returns `Replay` when
`response.hasBody()` is true; since messaging's sentinel is always bodyless,
**every completed messaging duplicate resolves to `EngineDecision.Unavailable`,
never `Replay`.** In HTTP, `Unavailable` is a terminal error (crash window,
oversized body — mapped to a 409 the client shouldn't retry). In messaging,
because of the sentinel choice above, `Unavailable` becomes **the ordinary,
expected outcome for every routine duplicate skip** — not an error. See the
action-mapping table below: it's mapped to ack-and-skip, the same action
`Replay` would have gotten had dedupe-only not been the v1 scope.

### 3. Generalize `EffectiveKey`/`Fingerprint`/Postgres schema: `route` + `handler` (done — Slice 034)

`CONTEXT.md`'s "endpoint" (HTTP method+path together, the operation
identity) generalizes, for messaging, to two dimensions — not one, so two
listeners sharing a destination stay isolated, mirroring how two HTTP routes
never collide. Completed by Slice 034:

| Type | Was | Now |
|---|---|---|
| `model/EffectiveKey` | `(method, path, principal, value)` | `(route, handler, principal, value)` — `route` = where it came in on (HTTP path, or message destination/topic); `handler` = which code processes it (HTTP method+controller, or listener id/consumer group) |
| `fingerprint/Fingerprint.sha256(method, path, body)` | HTTP verb + URI + payload hash | `sha256(route, handler, body)` — algorithm (SHA-256, NUL-joined, canonical-JSON body normalization) is already payload-agnostic, only the param names changed |
| Postgres `idempotency_record` columns | `http_method, path, principal, idempotency_key, ...` | `route, handler, principal, idempotency_key, ...` — edited directly into `V1__idempotency_record.sql` (no V2 migration; the library was unreleased at the time, so there were no deployments to migrate through) |

`key/EffectiveKeyFactory.create(String route, String handler, String
principal, String value)` (in `blocks-idempotency-core`) is the resulting
transport-neutral construction seam. The subsequent module split (ADR 0006)
moved the `HttpServletRequest`-reading half out entirely, into
`web/key/HttpEffectiveKeyFactory.create(HttpServletRequest, ...)` and
`web/key/HeaderKeyStrategy.resolve(HttpServletRequest, ...)` (both in
`blocks-idempotency-web`) — both delegate to core's neutral factory rather
than constructing `EffectiveKey` directly, so a future messaging module's
own key resolver should do the same.

**Principal scope**: always `NO_PRINCIPAL` for v1 messaging — no servlet-
principal equivalent exists for a message listener. A future need (e.g. a
tenant-id message header) is an extension of `PrincipalClaimResolver`'s
existing opaque-claim escape hatch resolving from message headers instead of
`HttpServletRequest`, not a new concept.

### 4. New adapter module: `blocks-idempotency-messaging-kafka`

Single module for v1 — no `-messaging-core` split yet. Extract one once
RabbitMQ/JMS (Phase 3) reveal what's actually broker-agnostic vs.
Kafka-specific; guessing the boundary now, with only one data point, isn't
worth it. `-rabbit`/`-jms` remain the Phase 3 destination, not v1 targets.

What the module needs to build, mirroring the existing `-store-redis`/
`-store-postgres` pattern of a thin adapter over the shared core:

- **Wrapping mechanism** — a Spring AOP `@Around` advice (`MethodInterceptor`)
  around `@Idempotent` + `@KafkaListener` methods, the same mechanism
  `@Transactional` already uses on listener methods. No buffered-request
  wrapping needed — the message payload is already a plain method argument.
- **Key extraction** — from `ConsumerRecord.headers()` (header strategy) or
  the deserialized payload (body-field strategy, reusing
  `BodyFieldKeyStrategy` unchanged).
- **Startup validation** — a `KafkaListenerEndpointRegistry`-driven scan for
  `@Idempotent` + `@KafkaListener` methods, mirroring
  `IdempotentHandlerValidator`'s key-strategy/ttl/store checks, plus the
  extra WAIT-rejection rule from ADR 0005.

### 5. `RejectReason` → consumer action

`RejectReason` already lives in the framework-agnostic `engine/` package —
no rework needed there, only the translation layer:

| Outcome | HTTP today | Messaging action | Why |
|---|---|---|---|
| `IN_PROGRESS` (REJECT) | 409 + Retry-After | **ack-and-skip** | The other in-flight delivery is already handling the effect; nacking would just cause a redundant redelivery loop. Safe because the primary's own message is natively redelivered by the broker if it fails. |
| `EngineDecision.Unavailable` (every completed dupe — see §2) | 409 terminal | **ack-and-skip** | Routine outcome in messaging, not an error — see §2. |
| `COLLISION` (fingerprint mismatch) | 422 | **dead-letter** | A key reused with a different payload is a producer bug or poison message — no consumer-side retry resolves it. |
| `KEY_REQUIRED` / `KEY_INVALID` | 400 | **dead-letter** | A structurally bad message; broker-native redelivery can't fix a message that will never carry a valid key. |
| `STORE_UNAVAILABLE` (`onStoreFailure=CLOSED`) | 503 | **nack-with-backoff** | Transient infrastructure trouble, not a poison message (dead-letter would be wrong) and it's unknown whether the effect already ran (ack-and-skip would risk silently losing the message). |
| `RELEASED` / `TIMEOUT` (WAIT outcomes) | 409 + Retry-After | **N/A** | `whenInProgress=WAIT` is disabled for v1 — see §6 / ADR 0005. |

### 6. `whenInProgress=WAIT` disabled for v1

Full reasoning in `docs/adr/0005-messaging-wait-disabled.md`. Summary:
enforced via a startup validation rule (same shape as
`IdempotentHandlerValidator`'s existing rules) rather than silent coercion.
Kept out because blocking a Kafka listener container thread inside
`store.await()` risks missing `max.poll.interval.ms` and triggering a
partition rebalance — a worse failure mode than HTTP thread-pool pressure —
and is arguably redundant for messaging specifically, since a failed
primary's own message is natively redelivered by the broker at-least-once
(unlike HTTP, which has no such mechanism). Trade-off accepted: loses WAIT's
narrow correctness fallback, and is a transport-conditional exception to
`CONTEXT.md`'s "single mechanism, same policy" principle.

### 7. Store implications

- **Redis**: no changes needed.
- **Postgres**: schema generalized per §3. The thread-bound-transaction
  constraint (`reserve()` opens a transaction that `complete()`/`release()`
  must finish on the same thread) is **not a new problem** — it already
  excludes WebFlux/async servlet handlers per the store's own javadoc, and a
  standard synchronous, single-container-thread `@KafkaListener` satisfies
  the same assumption a synchronous MVC controller does today. Document this
  as a carried-over constraint; it would still break under reactive
  consumers (Reactor Kafka, deferred manual-ack to another thread).

## Recommended Phased Build Order

1. ~~**Generalize `core`** — rename `EffectiveKey`/`Fingerprint` fields and
   the Postgres schema per §3, additively.~~ **Done (Slice 034).** A
   follow-on structural step also happened since: `core` is now genuinely
   transport-neutral end to end — HTTP integration moved to a new
   `blocks-idempotency-web` module (ADR 0006), so a future
   `blocks-idempotency-messaging-kafka` module depends on `core` alone, the
   same way `blocks-idempotency-web`/`-store-redis`/`-store-postgres` do.
2. **`blocks-idempotency-messaging-kafka` end-to-end**, including the AOP
   advice mechanism, the WAIT-rejection startup rule, and the
   action-mapping table in §5 — validated against real listener-container
   behavior (rebalance/redelivery edge cases). Should mirror
   `blocks-idempotency-web`'s `@AutoConfigureAfter(IdempotencyAutoConfiguration.class)`
   + `@ConditionalOnBean(IdempotencyEngineRegistry.class)` auto-configuration
   ordering pattern (ADR 0006) rather than inventing a new one.
3. **Extend the pattern to RabbitMQ and JMS**, factoring out whatever proves
   genuinely broker-agnostic from the Kafka work into a shared
   `-messaging-core` module at that point.

## Critical Files (for whichever phase is picked up next)

- `blocks-idempotency-core/src/main/java/io/adzubla/blocks/idempotency/engine/IdempotencyEngine.java` — the reusable orchestration core
- `blocks-idempotency-core/src/main/java/io/adzubla/blocks/idempotency/model/EffectiveKey.java` — the `route`/`handler` shape (Slice 034)
- `blocks-idempotency-core/src/main/java/io/adzubla/blocks/idempotency/key/EffectiveKeyFactory.java` — the transport-neutral construction seam to reuse
- `blocks-idempotency-web/src/main/java/io/adzubla/blocks/idempotency/web/IdempotencyInterceptor.java` — reference for the AOP advice this becomes
- `blocks-idempotency-web/src/main/java/io/adzubla/blocks/idempotency/web/config/IdempotencyWebAutoConfiguration.java` — reference for the wiring pattern (auto-configuration ordering) a messaging adapter should replicate
- `blocks-idempotency-store-postgres/src/main/resources/db/migration/V1__idempotency_record.sql` — already generalized (Slice 034), no further schema change expected for Kafka

## Verification (once a phase is picked up)

- **Phase 1** (core generalization): run `mvn test -pl blocks-idempotency-core`
  — the shared `IdempotencyStoreContractTest` suite must still pass unchanged
  against `InMemoryIdempotencyStore` after the field rename, proving it was
  additive.
- **Phase 2** (Kafka module): an embedded-Kafka integration test exercising a
  duplicate delivery (same key, same/different payload) against a real
  `@KafkaListener` + `@Idempotent`, asserting the full action-mapping table
  in §5 (ack-and-skip / dead-letter / nack-with-backoff), plus a Postgres
  integration test confirming the renamed schema and the thread-bound
  transaction join still work end-to-end via `mvn test -pl
  blocks-idempotency-store-postgres -am`. A startup-validation test
  confirming `whenInProgress=WAIT` on a `@KafkaListener` method fails fast
  (ADR 0005).
