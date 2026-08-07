# Idempotency for Spring Boot

A drop-in idempotency mechanism for Spring Boot REST endpoints *and* message
listeners (Kafka, RabbitMQ, JMS). Mark a handler or listener `@Idempotent`, pick
where the key comes from and which store backs it, and the library takes care of
the rest: caching the first response (HTTP) or deduping the delivery (messaging),
rejecting or waiting on concurrent duplicates, detecting a key reused for a
different payload, and degrading sanely if the store goes down.

## Why use this

Retries, double-clicks, and at-least-once message redelivery all cause the same
class of bug: an operation runs more than once (a payment charged twice, an order
created twice). Teams usually reinvent this with an ad-hoc unique constraint or a
"check before insert," and rarely handle the hard parts — concurrent duplicates in
flight, replaying the *original* response, telling a genuine retry apart from a key
reused for a different request, and behaving well when the store is unavailable.

This library gives you that as a single annotation:

```java
@PostMapping("/orders")
@Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
public ResponseEntity<Order> createOrder(@RequestBody OrderRequest request) { ... }
```

A repeat request with the same effective key returns the original response
(status, headers, body) instead of re-executing the handler, flagged with
`Idempotency-Replayed: true`.

The same annotation also protects a `@KafkaListener`/`@RabbitListener`/`@JmsListener`
method against redelivery — a broker resending the same message (consumer restart,
missed ack, redelivery policy) acks and skips the duplicate instead of re-running the
listener body. See [Messaging listeners](#messaging-listeners) below; the HTTP
sections above and below cover the `web` module.

**Use it when:**
- Clients may retry the same request (network timeouts, double-clicks, message redelivery).
- The operation has a side effect that must not run twice (charge, create, decrement stock).

**Skip it when:** the operation is naturally idempotent already (e.g. a `PUT` that
just overwrites state), or is a pure read.

**Backed by Redis or Postgres** — pick per endpoint/listener. Redis is
best-effort and fast, for effects external to your database (payment gateway
calls, emails). Postgres is a real exactly-once guarantee for effects that
write to that same database, by joining the handler's own transaction. Both
can be registered at once in the same application. See [Stores](#stores) for
the full comparison.

## How it works

`blocks-idempotency-core` holds one policy engine and `IdempotencyEngine`/
`IdempotencyStore` SPI shared by both transports; `web` and the messaging modules
each adapt it to their own request/delivery shape. The mechanics below are the
same either way, with the differences noted inline (and detailed in
[Messaging listeners](#messaging-listeners)):

- **Effective key** — one key per request/delivery, resolved by a strategy you
  choose per endpoint/listener:
  - `header` — the key comes from a client-supplied header (HTTP) or broker
    header/property (messaging), e.g. `X-Idempotency-Key`. Covers retries/double-clicks
    or broker redelivery of the same message.
  - `fieldPath` — a JSONPath into the request/message body (e.g. `$.order.id`).
    Covers callers you don't control, deduplicated by business identity.

  Exactly one of the two is required — validated at application startup.
- **Scope** — route + key value, plus the authenticated principal for HTTP
  (messaging has no principal equivalent, so it's route/topic/queue + listener id
  + key value). The same key value on two different routes/listeners, or from two
  different users, never collides.
- **Collision** — same key, different body (by a method+route+body fingerprint) →
  `422` (HTTP) or dead-lettered (messaging).
- **Concurrency** — a second request/delivery finding the key in-progress either
  gets an immediate reject (`whenInProgress = REJECT`, the default) or blocks for
  the primary's result (`WAIT`, HTTP only — see [Messaging listeners](#messaging-listeners)).
- **Caching** — HTTP: only `2xx` responses are cached; any error response, or the
  handler throwing, releases the key so a genuine retry can proceed. Messaging is
  dedupe-only (ADR 0004) — nothing is cached or replayed, a duplicate delivery is
  just acked and skipped.
- **Expiration** — a default 24h TTL (configurable). An expired key behaves as a
  brand-new one.
- **Store failure** — fail-open by default (request/delivery goes through
  unprotected); fail-closed is opt-in per endpoint/listener (`503` for HTTP,
  message left for broker redelivery for messaging).

See `CONTEXT.md` for the full glossary and `docs/adr/` for the design rationale.

### Request/delivery flow

One `IdempotencyEngine`/`IdempotencyStore` decision flow, shared by both
transports — only the adapter at the edges differs: `IdempotencyInterceptor`
(HTTP, `web`) or a broker's `@Aspect` extending `AbstractMessagingIdempotencyAdvice`
(Kafka/RabbitMQ/JMS, `messaging-core`). Each `-->>` step below shows the HTTP
outcome and, where it differs, the messaging outcome after a slash.

```mermaid
sequenceDiagram
    autonumber
    actor Caller as Client / Broker
    participant Adapter as IdempotencyInterceptor / *IdempotencyAdvice
    participant Registry as IdempotencyEngineRegistry
    participant Engine as IdempotencyEngine
    participant Store as IdempotencyStore
    participant Handler as Handler / Listener method

    Caller->>Adapter: HTTP request / message delivery
    Adapter->>Adapter: resolve @Idempotent policy (store/ttl/onStoreFailure/whenInProgress/keyRequired)
    Adapter->>Adapter: resolve raw key (header or fieldPath)

    alt key missing & required
        Adapter-->>Caller: 400 key_required / dead-lettered
    else key invalid (charset/length)
        Adapter-->>Caller: 400 key_invalid / dead-lettered
    else key present & valid
        Adapter->>Adapter: build EffectiveKey (route+principal, or destination+listenerId, +key)
        Adapter->>Adapter: compute fingerprint (route+body)
        Adapter->>Registry: engine(store qualifier)
        Registry-->>Adapter: IdempotencyEngine

        Adapter->>Engine: before(key, fingerprint, lockTtl, onStoreFailure, whenInProgress, waitTimeout)
        Engine->>Store: reserve(key, fingerprint, lockTtl)

        alt store unavailable
            Store-->>Engine: StoreUnavailableException
            alt onStoreFailure=CLOSED
                Engine-->>Adapter: FailClosed
                Adapter-->>Caller: 503 store_unavailable / left un-acked for redelivery
            else onStoreFailure=OPEN
                Engine-->>Adapter: ProceedUnprotected
                Adapter->>Handler: invoke (unprotected)
                Handler-->>Caller: original response / acked
            end
        else RESERVED (fresh key)
            Store-->>Engine: ReservationResult(RESERVED, fenceToken)
            Engine-->>Adapter: Proceed(key, fenceToken)
            Adapter->>Handler: invoke
            Handler-->>Adapter: response captured / returns normally

            alt 2xx response (HTTP) or normal return (messaging)
                Adapter->>Engine: complete(key, fenceToken, response, ttl)
                Engine->>Store: complete(key, fenceToken, response, ttl)
                Adapter-->>Caller: original response / acked
            else non-2xx or exception thrown
                Adapter->>Engine: release(key, fenceToken)
                Engine->>Store: release(key, fenceToken)
                Adapter-->>Caller: original error response / exception propagates for redelivery
            end
        else EXISTING record found
            Store-->>Engine: ReservationResult(existing record)
            alt fingerprint mismatch
                Engine-->>Adapter: Collision
                Adapter-->>Caller: 422 collision / dead-lettered
            else existing.completed
                Engine-->>Adapter: Replay(cachedResponse) or Unavailable
                alt response cached (HTTP only — messaging never caches, ADR 0004)
                    Adapter-->>Caller: 2xx replay (Idempotency-Replayed: true)
                else response not replayable / routine dedupe skip
                    Adapter-->>Caller: 409 response_unavailable / acked (skipped, not invoked)
                end
            else still in-progress, whenInProgress=REJECT
                Engine-->>Adapter: Reject(IN_PROGRESS, retryAfter)
                Adapter-->>Caller: 409 in_progress + Retry-After / acked (skipped, not invoked)
            else still in-progress, whenInProgress=WAIT (HTTP only — ADR 0005 rejects WAIT at startup for messaging)
                Engine->>Store: await(key, waitTimeout, pollInterval, pollJitter)
                Store-->>Engine: completed record / empty / still in-progress
                alt primary completed
                    Engine-->>Adapter: Replay(cachedResponse) or Unavailable
                    Adapter-->>Caller: 2xx replay or 409 response_unavailable
                else primary released (error) mid-wait
                    Engine-->>Adapter: Reject(RELEASED, retryAfter)
                    Adapter-->>Caller: 409 released + Retry-After
                else waitTimeout elapsed
                    Engine-->>Adapter: Reject(TIMEOUT, retryAfter)
                    Adapter-->>Caller: 409 timeout + Retry-After
                end
            end
        end
    end
```

HTTP has one extra step this diagram omits for clarity: `IdempotencyFilter` wraps
the request/response to buffer the body before `IdempotencyInterceptor` runs, and
copies the captured body back to the real response afterwards — see
[Messaging listeners](#messaging-listeners) for the messaging-specific mechanics
(dead-letter publishing, fail-closed semantics per broker) this diagram
generalizes over.

## Install

Group id `io.adzubla.blocks`. Artifacts:

| Artifact                             | Purpose                                                                            |
|---------------------------------------|-------------------------------------------------------------------------------------|
| `blocks-idempotency-core`             | Transport-neutral: `@Idempotent`, policy engine, `IdempotencyStore` SPI            |
| `blocks-idempotency-web`              | Spring MVC/Servlet integration (filter, interceptor, exception handling)          |
| `blocks-idempotency-messaging-core`   | Broker-neutral messaging advice skeleton (pulled in transitively by the three below) |
| `blocks-idempotency-messaging-kafka`  | `@KafkaListener` integration                                                       |
| `blocks-idempotency-messaging-rabbitmq` | `@RabbitListener` integration                                                    |
| `blocks-idempotency-messaging-jms`    | `@JmsListener` integration                                                         |
| `blocks-idempotency-store-redis`      | Redis-backed `IdempotencyStore`                                                    |
| `blocks-idempotency-store-postgres`   | Postgres-backed `IdempotencyStore`                                                 |

Add `core` plus whichever transport module(s) (`web` and/or one or more
`messaging-*`) and store module(s) your application uses — each is optional so
you don't pull in Kafka, RabbitMQ, JMS, Redis, or JDBC you don't need. A single
application can mix transports (HTTP endpoints and message listeners side by
side) and stores (see below).

```xml
<dependency>
    <groupId>io.adzubla.blocks</groupId>
    <artifactId>blocks-idempotency-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
    <groupId>io.adzubla.blocks</groupId>
    <artifactId>blocks-idempotency-web</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
<!-- for message listeners instead of/alongside web, add one or more: -->
<dependency>
    <groupId>io.adzubla.blocks</groupId>
    <artifactId>blocks-idempotency-messaging-kafka</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
    <groupId>io.adzubla.blocks</groupId>
    <artifactId>blocks-idempotency-store-redis</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Each store module auto-configures its `IdempotencyStore` bean under a qualifier
(`"redis"` / `"postgres"`) as soon as its Spring Boot starter (`spring-boot-starter-data-redis`
/ `spring-boot-starter-jdbc` + a `DataSource`) is on the classpath and configured.
`core` never provides a default store — you need at least one store module. Both
modules can be on the classpath at once: each `@Idempotent(store = ...)` routes
independently to the store it names, so one application can back some endpoints
with Redis and others with Postgres.

Each messaging module likewise auto-configures its advice as soon as its
listener annotation (`@KafkaListener`/`@RabbitListener`/`@JmsListener`) is on the
classpath and an `IdempotencyEngineRegistry` bean exists (i.e. `core` plus at
least one store module). All three can be on the classpath at once, alongside
`web` — the same `@Idempotent` annotation just gets picked up wherever it's
paired with a listener/handler annotation the corresponding module knows about.

## `@Idempotent` reference

```java
@Idempotent(
    header = Idempotent.IDEMPOTENCY_KEY_HEADER,  // "X-Idempotency-Key"; xor fieldPath = "$.order.id"
    store = RedisIdempotencyStore.QUALIFIER,     // "" inherits idempotency.default-store
    ttl = "PT1H",                                // "" inherits idempotency.default-ttl
    keyRequired = true,                          // fixed default, does not inherit
    onStoreFailure = OnStoreFailure.CLOSED,      // DEFAULT inherits the global posture
    whenInProgress = WhenInProgress.WAIT         // DEFAULT inherits the global posture
)
```

Global defaults live under `idempotency.*` in `application.properties`:

```properties
# store qualifier used when @Idempotent leaves store=""; no default - required
# unless every @Idempotent sets store= explicitly
idempotency.default-store=redis
# response TTL used when @Idempotent leaves ttl=""
idempotency.default-ttl=24h
# posture when the store is unavailable: OPEN lets the request through, CLOSED returns 503
idempotency.default-on-store-failure=OPEN
# behavior for a concurrent in-progress key: REJECT returns 409, WAIT blocks for the primary's result
idempotency.default-when-in-progress=REJECT
# how long a reservation is held before an abandoned one is treated as gone (anti-poisoning)
idempotency.lock-ttl=30s
# how long a WAIT caller blocks for the primary's result before giving up
idempotency.wait-timeout=5s
# base delay between WAIT-mode polls, for stores without a native blocking
# wait (Postgres has one and ignores both of these)
idempotency.poll-interval=100ms
# extra random delay added to each poll, up to this much
idempotency.poll-jitter=50ms
# responses over this size aren't cached; a replay then gets 409 response_unavailable
idempotency.max-body-size=1MB
# max length of the raw key value (header or body-field); longer values are
# rejected with 400, as is any value outside the fixed charset [A-Za-z0-9_.:-]+
idempotency.key.max-length=255
# fold the authenticated principal into the key scope
idempotency.scope.principal-enabled=true
# opaque value passed to the active PrincipalClaimResolver bean; the default
# resolver ignores it and always scopes by Principal#getName() - see "Scoping
# by a custom principal claim" below
idempotency.scope.principal-claim=sub
# header used to flag a replayed response
idempotency.replay.header-name=Idempotency-Replayed
# headers stripped from a replay; Set-Cookie is always stripped regardless of this list
idempotency.replay.header-denylist=Date,Set-Cookie,traceparent,tracestate
# emit counters for replay/collision/concurrency/fail-open outcomes (needs a MeterRegistry)
idempotency.metrics.enabled=true
```

A misconfigured `@Idempotent` (both/neither of `header`/`fieldPath`, an invalid
TTL, or a `store` qualifier with no matching bean) fails application startup, not
the first request.

### Scoping by a custom principal claim

By default the key scope's principal is `Principal#getName()` off
`HttpServletRequest.getUserPrincipal()`. To scope by something else instead
(e.g. a JWT claim, when the request's principal is your auth stack's own JWT
token type), register a `PrincipalClaimResolver` bean — `core` has no
dependency on any particular auth library, so this is left to the application:

```java
@Bean
PrincipalClaimResolver principalClaimResolver() {
    return (principal, claim) -> principal instanceof JwtAuthenticationToken jwt
            ? jwt.getToken().getClaimAsString(claim)
            : null; // null falls back to Principal#getName()
}
```

`claim` is whatever `idempotency.scope.principal-claim` is set to (default
`"sub"`), passed through verbatim — the library never inspects it itself. With
no such bean registered, `DefaultPrincipalClaimResolver` is used, which ignores
`claim` and always returns `Principal#getName()`.

## Messaging listeners

Add `blocks-idempotency-messaging-kafka` / `-rabbitmq` / `-jms` (any combination)
alongside `core` and a store module, then stack `@Idempotent` on a listener method
the same way you would on a controller handler. Everything in
[`@Idempotent` reference](#idempotent-reference) above applies, with three
messaging-specific differences (see [Request/delivery flow](#requestdelivery-flow) and
ADRs [0004](docs/adr/0004-messaging-dedupe-only-v1-scope.md) /
[0005](docs/adr/0005-messaging-wait-disabled.md)):

- **No response replay** — a duplicate delivery is acked and skipped, not replayed.
  There's no captured response to hand back to a message listener the way there is
  to an HTTP client.
- **`whenInProgress = WAIT` is rejected at startup** — blocking a listener
  container thread on another delivery's result risks a broker-side timeout
  (e.g. a Kafka partition rebalance). Only `REJECT` is supported.
- **Terminal failures are dead-lettered, not turned into an HTTP status** — a
  missing/invalid key or a fingerprint collision routes the message to a
  dead-letter topic/queue (Kafka, JMS: republished by the library; RabbitMQ: the
  broker's own dead-letter config, via a reject-without-requeue) instead of a
  `422`/`400` response.

The effective key's scope is destination (topic/queue) + listener id + key value —
there's no HTTP-style authenticated principal to fold in. The listener id is the
listener annotation's own `id()` if set, else the method's fully-qualified name.

### Kafka

```java
@Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
@KafkaListener(id = "orders-listener", topics = "orders")
void onOrderCreated(ConsumerRecord<String, String> record) {
    orderService.create(record.value());
}
```

The listener method must accept a `ConsumerRecord<?, ?>` parameter — the advice
reads the header and body off it for key resolution and fingerprinting (validated
at startup). A collision or invalid key republishes to `<topic><idempotency.kafka.dead-letter-suffix>`
(default `.DLT`) via an optionally-injected `KafkaTemplate`.

```properties
# suffix appended to a delivery's source topic to form its dead-letter topic
idempotency.kafka.dead-letter-suffix=.DLT
```

### RabbitMQ

```java
@Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
@RabbitListener(id = "orders-listener", queues = "orders")
void onOrderCreated(org.springframework.amqp.core.Message message) {
    orderService.create(message.getBody());
}
```

The listener method must accept a Spring AMQP `Message` parameter. RabbitMQ
dead-letters natively: a collision, invalid key, or fail-closed store outage
throws `AmqpRejectAndDontRequeueException`, and the broker routes the rejected
message per whatever dead-letter exchange/queue you've configured on the queue
itself — this module doesn't own that configuration.

### JMS

```java
@Idempotent(header = "IdempotencyKey")
@JmsListener(id = "orders-listener", destination = "orders")
void onOrderCreated(jakarta.jms.Message message) throws JMSException {
    orderService.create(message.getBody(String.class));
}
```

The listener method must accept a `jakarta.jms.Message` parameter. Unlike the HTTP
default header name, JMS property names must be Java-identifier-like (JMS message
selector syntax, no hyphens) — use something like `IdempotencyKey` rather than
`X-Idempotency-Key`. A collision or invalid key republishes to
`<destination><idempotency.jms.dead-letter-suffix>` (default `.DLQ`) via an
optionally-injected `JmsTemplate`.

```properties
# suffix appended to a delivery's source destination to form its dead-letter destination
idempotency.jms.dead-letter-suffix=.DLQ
```

## Stores

|             | Redis                                                                   | Postgres                                                      |
|-------------|-------------------------------------------------------------------------|---------------------------------------------------------------|
| Guarantee   | Best-effort                                                             | Exactly-once, if the effect writes to the same database       |
| Concurrency | Polling (~100ms + jitter)                                               | Native row lock, blocks until the holder commits/rolls back   |
| Good for    | External or non-transactional effects (calls to other services, emails) | Effects that are themselves a write to this Postgres database |

Store choice is per endpoint/listener, not per application: register both
modules and set `store` on each `@Idempotent` to pick Redis for one handler and
Postgres for another in the same service. The Postgres store's exactly-once
guarantee (below) extends to message listeners too — a synchronous,
single-session-thread `@KafkaListener`/`@RabbitListener`/`@JmsListener` satisfies
the same thread-bound transaction assumption a synchronous MVC controller does.

### In-memory store

`InMemoryIdempotencyStore` (in `core`) is the reusable test double used by the
library's own test suite. It is **not** auto-configured and not meant for
production — state is a local `ConcurrentHashMap`, lost on restart and not shared
across instances. Register it yourself only for tests or a single-instance
prototype.

### Redis

Qualifier `"redis"`. Best-effort: fast, but a Redis blip or eviction can lose a
reservation. One Redis **Hash** per record, keyed by a SHA-256 of
method/path/principal/key — a single key per record, so it works under Redis
Cluster with no cross-slot operations. Reserve and complete are atomic Lua
scripts. Lifecycle rides Redis's own key TTL: reserve → `lock-ttl` (~30s),
complete → the response TTL, failure → delete.

Requires `spring-boot-starter-data-redis` and a configured `StringRedisTemplate`
(standard `spring.data.redis.*` properties).

```properties
# prefix for the hashed record key; namespace multiple apps sharing one Redis instance
idempotency.redis.key-prefix=idempotency:
```

**Use when** the protected effect is external or non-transactional (a payment
gateway call, sending an email, calling another service) — you get fast
protection without a false promise of atomicity with your own database.

**`lock-ttl` must outlast your slowest handler.** Because lifecycle rides the
key's own TTL, a legitimately-slow-but-alive primary that outruns `lock-ttl`
has its key reclaimed before it calls `complete()` — the response is silently
never cached (the effect still ran; only the cache write is lost), same as if
the primary had crashed. Set `lock-ttl` comfortably above worst-case handler
duration. `RedisIdempotencyStore` logs a `WARN` when `complete()` no-ops this
way, so the gap is visible rather than silent.

### Postgres

Qualifier `"postgres"`. Exactly-once *for effects that write to the same
database*: `reserve()` opens the transaction the handler's effect runs in (it
joins transparently via ordinary `@Transactional`/plain JDBC on the same
thread); `complete()` commits response + effect together; `release()` rolls back
both. Concurrency is native — a second reservation blocks on the row's
`UNIQUE`/primary-key conflict until the first commits or rolls back, bounded by
`lock_timeout`. `whenInProgress = WAIT` uses this same native block instead of
polling, so `idempotency.poll-interval`/`idempotency.poll-jitter` don't apply
here — a waiter resolves the instant the primary commits or rolls back, not on
the next poll tick.

Requires `spring-boot-starter-jdbc`, a `DataSource`/`PlatformTransactionManager`,
and the `idempotency_record` table. A Flyway migration
(`V1__idempotency_record.sql`) ships with the module (optional dependency) —
either let Flyway auto-run it or apply it with your own migration tool. A
built-in scheduled job sweeps expired rows (Postgres has no native TTL).

```properties
# upper bound a waiter blocks on the reservation row's native lock before giving up
idempotency.postgres.lock-timeout=2s
# disable to sweep expired rows with your own job/cron instead
idempotency.postgres.cleanup.enabled=true
# how often the sweep runs
idempotency.postgres.cleanup.interval=5m
```

**Use when** the protected effect is a write to this same Postgres database (e.g.
creating an order row) — you get a real exactly-once guarantee, not just
best-effort caching. Not currently supported with async handlers
(`Callable`/`DeferredResult`/WebFlux) since those can hop threads mid-request,
which breaks the thread-bound transaction this store relies on.

## Status codes at a glance

This section is HTTP-specific (`web` module) — a message listener never returns
a status code; see [Messaging listeners](#messaging-listeners) for its
dead-letter/fail-closed/skip outcomes instead.

Every rejecting outcome is thrown by the interceptor as a typed
`IdempotencyException` rather than written to the response directly, so it
flows through Spring's normal exception-handling machinery just like an
exception thrown from a controller method. A default `@ControllerAdvice`
(`IdempotencyExceptionHandler`, auto-registered by the library) turns each one
into the bare status(+headers)/no-body response below. Every non-2xx response
carries `Idempotency-Reject-Reason` — a single closed vocabulary uniform
across all of them, so a client can always tell the root cause apart from any
other error the application itself might return at that status code,
without parsing a body. Only the in-progress-duplicate case additionally
carries `Retry-After`, since it alone is the "resend with the same key" case.
The only response the library ever writes a body for is a `2xx` replay, which
reproduces the original captured response verbatim.

| Status                | Meaning                                                                           | Exception                                 | `Idempotency-Reject-Reason`      | Other headers                          | Body                   |
|-----------------------|-----------------------------------------------------------------------------------|-------------------------------------------|----------------------------------|----------------------------------------|------------------------|
| `2xx`                 | Fresh execution, or replay of a cached response                                   | —                                         | —                                | `Idempotency-Replayed: true` on replay | Original response body |
| `409` + `Retry-After` | In-progress duplicate (reject/wait-timeout/released) — safe to retry the same key | `IdempotencyConflictException`            | `in_progress\|released\|timeout` | `Retry-After`                          | empty                  |
| `409`                 | Effect completed but the response can't be replayed (terminal — don't retry)      | `IdempotencyResponseUnavailableException` | `response_unavailable`           | none                                   | empty                  |
| `422`                 | Same key, different payload (fingerprint collision)                               | `IdempotencyCollisionException`           | `collision`                      | none                                   | empty                  |
| `400`                 | Key required but missing                                                          | `IdempotencyKeyRequiredException`         | `key_required`                   | none                                   | empty                  |
| `400`                 | Key value too long, or outside the allowed charset                                | `IdempotencyKeyInvalidException`          | `key_invalid`                    | none                                   | empty                  |
| `503`                 | Store unavailable and `onStoreFailure = CLOSED`                                   | `IdempotencyFailClosedException`          | `store_unavailable`              | none                                   | empty                  |

### Overriding error responses

Because these are ordinary exceptions, an application's own
`@ControllerAdvice` can override any single one of them — just declare an
`@ExceptionHandler` for the same exception type (or for the common
`IdempotencyException` supertype). The library's own advice is registered at
`@Order(Ordered.LOWEST_PRECEDENCE)`, so an application handler for the same
type always wins, letting you wrap idempotency errors in your standard error
body instead of the library's default empty one:

```java
@ControllerAdvice
class ApiErrorHandler {

    @ExceptionHandler(IdempotencyCollisionException.class)
    ResponseEntity<ErrorBody> handleCollision(IdempotencyCollisionException ex) {
        return ResponseEntity.status(ex.status())
                .body(new ErrorBody("IDEMPOTENCY_COLLISION", ex.getMessage()));
    }
}
```
