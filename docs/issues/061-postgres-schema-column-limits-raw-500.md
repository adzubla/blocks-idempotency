# Slice 061 — Postgres column limits narrower than accepted input → raw 500

> Source: system analysis (2026-11-19) · Type: AFK
> Status: needs-triage

## What to build

The Postgres schema stores the raw key components:
`route VARCHAR(512)`, `handler VARCHAR(255)`, `principal VARCHAR(255)`,
`idempotency_key VARCHAR(255)`. The application accepts more than that:

- `idempotency.key.max-length` is configurable and is not capped at 255.
  Raising it (for example to 512) makes every key longer than 255 characters
  fail to insert.
- The route is the request URI, whose length is bounded only by the container
  (8KB by default on Tomcat). A long URI (many path segments or long path
  variables) is longer than 512.
- The principal comes from a configurable claim (`principal-claim`), or
  `Principal.getName()` as a fallback. Neither has a length bound.

The insert fails with SQLState 22001 (`value too long`), surfaced as a
`DataIntegrityViolationException`. `asStoreUnavailableIfConnectionFailure`
deliberately doesn't treat that as an outage, so it escapes `engine.before` as a
**raw 500**, bypassing `onStoreFailure`. Redis is unaffected because it hashes
the key (`EffectiveKey.digestBytes`).

Candidate fixes, for triage:
- Startup validation: fail fast if `key.max-length` exceeds the Postgres column size.
- Store a digest for the unbounded components (route/principal), as Redis does,
  or widen them to `TEXT`. Beware the btree index-row limit (~2.7KB) if widening.
- Map 22001 to a client error (400/414) rather than a 500.

## Acceptance criteria

- [ ] A key at the configured `max-length` always inserts, or startup rejects the configuration.
- [ ] An over-long route or principal never yields a raw 500.
- [ ] Migration provided if the schema changes.

## Blocked by

—
