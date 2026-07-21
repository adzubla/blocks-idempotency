# Transport-neutral core: HTTP integration moves to `blocks-idempotency-web`

## Status

accepted

## Context and decision

`blocks-idempotency-core` bundled the transport-neutral engine (`engine/`,
`policy/`, `store/`, `model/`, `fingerprint/`, `metrics/`) together with a
full HTTP/Servlet integration (`web/`, `response/`, `validation/`, plus the
HTTP-specific half of `key/`) in one module. The messaging PRD
(`docs/prd/messaging-extension.md`) and ADRs 0004/0005 already assume future
`blocks-idempotency-messaging-kafka`/`-rabbit`/`-jms` modules depend on a
*neutral* core, mirroring how `-store-redis`/`-store-postgres` already
depend on core today for the store SPI — but that assumption wasn't
literally true: core still hard-depended on `jakarta.servlet`/`spring-webmvc`,
so a Kafka-only Spring Boot app (no web application context at all) would
have dragged in an unused servlet stack.

We decided to extract a new `blocks-idempotency-web` module, mirroring the
existing store-module pattern: a thin transport adapter depending on
`blocks-idempotency-core`. Everything HTTP-specific moved — `IdempotencyFilter`,
`IdempotencyInterceptor`, `CachedBodyHttpServletRequest`,
`IdempotencyException`/its 6 subtypes, `IdempotencyExceptionHandler`,
`ResponseCapture`/`ResponseReplayer`, `IdempotentHandlerValidator`,
`HeaderKeyStrategy` (renamed to `web.key.HeaderKeyStrategy` to avoid
splitting the `key` package across module jars), and a new
`HttpEffectiveKeyFactory` (the `HttpServletRequest`-reading half of what was
`EffectiveKeyFactory`, now delegating to the transport-neutral factory
that stays in core). `IdempotencyAutoConfiguration` split the same way: core
keeps the engine/metrics/principal-resolver beans (no longer conditional on
a web application), and a new `IdempotencyWebAutoConfiguration` in the web
module owns the filter/interceptor/validator/exception-handler wiring,
ordered after core's via `@AutoConfigureAfter`+`@ConditionalOnBean` — the
same load-bearing-ordering technique the store modules already use via
`@AutoConfigureBefore(IdempotencyAutoConfiguration.class)`.

## Considered Options

- **Leave `web/` in core, add messaging support alongside it** (rejected) —
  every future transport-only app (Kafka, JMS, RabbitMQ) would carry a
  servlet dependency it never uses. Contradicts the messaging PRD's own
  stated design, which already assumed a neutral core.
- **Keep `HeaderKeyStrategy` in the `key` package inside the web module**
  (rejected) — this would split the `key` package's classes across two
  module jars (`BodyFieldKeyStrategy`/`KeyFormat`/`EffectiveKeyFactory` in
  core, `HeaderKeyStrategy` in web). Renamed to `web.key.HeaderKeyStrategy`
  instead, keeping every package's classes in exactly one module.

## Consequences

- HTTP consumers now depend on `blocks-idempotency-core` **and**
  `blocks-idempotency-web` (previously `core` alone provided HTTP
  integration) — a new required dependency for every existing user, though
  the library is still unreleased (`0.1.0-SNAPSHOT`), so there's no
  migration cost for real deployments yet.
- `IdempotencyAutoConfiguration` (core) keeps its class name/package
  unchanged specifically so the store modules' `@AutoConfigureBefore`
  references didn't need to change — a future transport module (Kafka, JMS,
  RabbitMQ) should follow the same `@AutoConfigureAfter(IdempotencyAutoConfiguration.class)`
  + `@ConditionalOnBean(IdempotencyEngineRegistry.class)` pattern
  `IdempotencyWebAutoConfiguration` establishes here, rather than inventing
  a new ordering mechanism per transport.
- `EffectiveKeyFactory.create(String route, String handler, String
  principal, String value)` (core) is now the single, genuinely
  transport-neutral construction seam every adapter uses — `HttpEffectiveKeyFactory`
  (web) and any future messaging adapter's key resolver both delegate to it
  rather than constructing `EffectiveKey` directly.
