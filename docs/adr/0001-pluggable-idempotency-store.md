# Pluggable IdempotencyStore, with Redis and Postgres offering different guarantees

## Status

accepted

## Context and decision

Idempotency needs to persist the key reservation and the cached response. Storing
this in Redis is fast and brings native TTL and atomic locking, but writing the
effect (database) and writing to Redis are not atomic: there is a crash window
between committing the effect and writing the cache that can lead to re-execution —
i.e., *best-effort* idempotency only.

We decided to define an **`IdempotencyStore`** interface with **two
implementations**, and **each endpoint chooses** which to use
(e.g. `@Idempotent(store = ...)`):

- **`RedisIdempotencyStore`** (default) — fast, native TTL/lock, decoupled from the
  application database. *Best-effort* guarantee.
- **`PostgresIdempotencyStore`** — writes the idempotency record in the **same
  transaction** as the effect, delivering **exactly-once** *when the effect is a
  write to the database itself*. The reservation `INSERT` **is** the lock: a
  concurrent request with the same key blocks on the unique index until the first
  one commits (→ `unique_violation` → replay) or rolls back (→ executes). Primary
  crash = rollback = lock released by the database, no `lock-ttl`. The strong
  guarantee is on the **effect**; the **response** replay is best-effort (written
  in a later `UPDATE`, outside the effect's transaction).

We chose to name the store by database (**`Postgres`**), not `Jdbc`: the name tells
the truth about what is supported and tested, and unlocks Postgres features
(`ON CONFLICT`, `JSONB`, `TIMESTAMPTZ`, `BYTEA`). Support for another database is a
**new module** (`idempotency-store-mysql`), extracting a shared SQL base only when
a second concrete case exists (rule of two) — not a dialect abstraction guessed at
now.

## Modular packaging and store selection

Each implementation lives in a **separate JAR/module**; the `core` depends on none
of them. The consumer pulls only the module(s) it uses, and only those drag in
their dependencies (spring-data-redis, spring-jdbc). A new store in the future is a
new JAR, **with no change to the `core`**.

- **`idempotency-core`** — the `IdempotencyStore` interface, the annotation, filter
  + interceptor, and the policy engine. No store dependency.
- **`idempotency-store-redis`** — auto-configures a bean under the qualifier
  `"redis"`.
- **`idempotency-store-postgres`** — auto-configures a bean under the qualifier
  `"postgres"`.

Store selection on the annotation is by **`String` qualifier**
(`store="postgres"`), not by enum or `Class`. An enum would have to live in the
`core` and list stores that may not be on the classpath, requiring the `core` to be
edited for every new store — closing exactly the open world the pluggable interface
wants to keep. The String qualifier also solves the multi-datasource case (two
beans of the same database). The qualifier constant lives **in each store module**
(e.g. `RedisIdempotencyStore.QUALIFIER`), keeping the `core` agnostic. `store=""`
inherits the global configuration default.

## Considered Options

- **Redis only** — simple, but never delivers exactly-once; unsuitable for critical
  effects (money) that write to the database itself.
- **Postgres only** — atomic, but forces database infra on endpoints that don't
  touch the database, with no native TTL (requires a cleanup job) and contention on
  the hot database.
- **Pluggable interface with both impls** (chosen) — lets the guarantee be a
  per-endpoint decision, matching the library's opt-in nature.
- **Generic `JdbcIdempotencyStore` + dialect SPI** (rejected for now) — DRY if many
  databases appear, but an aspirational name and paying for the dialect abstraction
  before a second concrete database exists.
- **Enum `Store{REDIS, POSTGRES}` in the core** (rejected) — good autocomplete, but
  closes the set of stores and forces changes to the `core` for every new module.
- **`Class<? extends IdempotencyStore>` on the annotation** (rejected) — type-safe,
  but couples the consumer to the impl class at compile time and doesn't
  distinguish two beans of the same type (multi-datasource).

## Consequences

- The strong guarantee of `PostgresIdempotencyStore` **only holds if the effect is
  a write to the same database/transaction**. If the effect is an external call
  (payment gateway, email, another service), neither Postgres nor Redis provides
  atomicity — it falls back to best-effort + the external provider's idempotency.
- The per-endpoint store choice should be guided by: *the effect is a write to my
  database* → Postgres; *the effect is external or non-transactional* → Redis.
- The Postgres implementation requires a versioned schema (Flyway/Liquibase) and an
  expiration job; Redis does not. Concurrency is **native to the database** (block
  on the unique index), not the polling of ADR 0002 — which stays as the `core`
  default for Redis.
- Startup validation must fail fast if an endpoint references a store qualifier with
  no matching bean (module not included on the classpath).
