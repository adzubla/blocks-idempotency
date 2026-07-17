# Slice 023 — Bundle interceptor request-scoped state into one context object

> Source: code-review smell scan (2026-07-16) · Type: AFK
> Status: wontfix

## What to build

`IdempotencyInterceptor` (`IdempotencyInterceptor.java:58-61,126-144`) sets and
reads four separate request attributes (`ATTR_EFFECTIVE_KEY`, `ATTR_POLICY`,
`ATTR_FENCE_TOKEN`, `ATTR_ENGINE`) that always travel together — a data clump.
Bundle them into a single `RequestScopedIdempotencyContext(key, policy,
fenceToken, engine)` record stored under one request attribute, and update all
read/write sites to go through it.

## Acceptance criteria

- [ ] A single `RequestScopedIdempotencyContext` (or equivalent) type holds
      the four fields.
- [ ] `IdempotencyInterceptor` stores/retrieves exactly one request attribute
      instead of four.
- [ ] All existing call sites reading the individual attributes are updated;
      no dangling references to the old attribute keys remain.
- [ ] Existing web/interceptor tests continue to pass unmodified.

## Blocked by

—

## Comments

- Filed from a code-smell review of the codebase (Data Clumps).
