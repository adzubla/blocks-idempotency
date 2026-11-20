# 0007 — The Postgres schema is not auto-migrated by the library

Status: accepted (2026-11-20)

## Context

`blocks-idempotency-store-postgres` first shipped its table as a Flyway
migration, `db/migration/V1__idempotency_record.sql`, on Spring Boot's default
Flyway location. Any application with the jar on its classpath and Flyway
enabled ran it implicitly, in the application's own schema history. That breaks
for most real applications:

- Flyway versions are global across locations. A library-owned `V1` collides with
  the application's own `V1` ("Found more than one migration with version 1"), and
  against an existing history it fails validation (a lower, unapplied version, or a
  checksum mismatch).
- An application that sets `spring.flyway.locations` never finds the script, so
  the table is missing and the first request fails at runtime.
- Any later `V2` from the library would collide the same way.
- It applied DDL into the application's database without the application's
  review.

## Decision

The library ships the DDL but never runs it. The script lives at
`META-INF/blocks-idempotency/postgres/V1__idempotency_record.sql`, which no
migration tool scans by default, and the README reproduces the table. The
application copies it into its own migrations under its own version number. The
module no longer depends on Flyway outside its tests.

## Consequences

- No version or location collisions, and the application owns when and how the
  schema changes.
- The application must remember to create the table. Without it the first
  idempotent request on the Postgres store fails with `relation
  "idempotency_record" does not exist`; the README troubleshooting table covers
  it. A startup check that fails fast with a clear message is a possible follow-up.
- A library upgrade that changes the schema must be announced, and the
  application adds it as a new migration of its own.
- A test (`PostgresSchemaScriptShippingTest`) keeps the script off
  `classpath:db/migration`.
