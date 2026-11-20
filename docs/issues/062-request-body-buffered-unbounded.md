# Slice 062 — `@Idempotent` request bodies are buffered fully into memory with no size limit

> Source: system analysis (2026-11-19) · Type: AFK
> Status: needs-triage

## What to build

`CachedBodyHttpServletRequest.getBody()` calls
`super.getInputStream().readAllBytes()` with no cap. `IdempotencyInterceptor.preHandle`
calls `requestBody(request)` **before** resolving or validating the key, so even
a header-strategy request with a missing key, which is about to be rejected with
400, is buffered in full first. `max-body-size` only applies to the captured
**response** (`ResponseCapture`).

A client can therefore make the server buffer an arbitrarily large body per
request on any `@Idempotent` endpoint. Container limits (Tomcat `maxPostSize`)
only cover form posts, not raw JSON streams.

Candidate fixes, for triage:
- Add a request-side limit (reuse `max-body-size` or a new `max-request-body-size`)
  and reject an oversized body with 413 while buffering.
- For the header strategy, resolve and validate the key before buffering the
  body, so requests without a key are rejected without reading the body.

## Acceptance criteria

- [ ] A request body over the configured limit is rejected (413) without being fully buffered.
- [ ] A header-strategy request with a missing or invalid key is rejected without its body being read.
- [ ] Fingerprinting and body-field resolution unchanged for bodies within the limit.

## Blocked by

—
