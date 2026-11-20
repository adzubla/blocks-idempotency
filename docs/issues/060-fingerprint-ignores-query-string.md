# Slice 060 — Fingerprint ignores the query string → different requests replay instead of 422

> Source: system analysis (2026-11-19) · Type: AFK
> Status: needs-triage

## What to build

`HttpEffectiveKeyFactory.create` takes the route from `request.getRequestURI()`
(no query string), and `IdempotencyInterceptor.preHandle` fingerprints
`Fingerprint.sha256(route, method, body)`. Query parameters therefore
contribute to neither the effective key nor the fingerprint.

For a JSON-body endpoint that also takes `@RequestParam`s:

```
POST /transfers?amount=100   X-Idempotency-Key: k1   → 201 (executes)
POST /transfers?amount=900   X-Idempotency-Key: k1   → 201 replay (should be 422)
```

The second request is a different operation reusing a key, which CONTEXT.md
defines as a **collision** (422). Instead the client is told the 900 transfer
succeeded. Form posts are unaffected, because `canonicalForm(getParameterMap())`
already merges query and form parameters.

Fix direction: include the query string, canonicalized (parameters sorted, the
same way `canonicalForm` does it), in the fingerprint input for non-form
requests. Keep it out of the effective key, so a changed query is reported as a
collision rather than treated as a new key.

## Acceptance criteria

- [ ] Same key + same body + different query parameters → 422 collision.
- [ ] Same key + same body + the same query parameters in a different order → replay (canonicalized).
- [ ] Form-post behavior unchanged (query params not double-counted).
- [ ] Messaging fingerprints unchanged.

## Blocked by

—
