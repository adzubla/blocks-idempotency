-- Idempotency record for PostgresIdempotencyStore (ADR 0001, ADR 0003).
-- The composite PRIMARY KEY provides the atomic reservation; the reservation
-- write runs inside the effect's own transaction and IS the lock (left open by
-- reserve(), committed by complete() or rolled back by release()). A fresh
-- reservation attempt is a conditional UPSERT, not a bare INSERT ... DO
-- NOTHING: it may supersede an existing row whose expires_at has passed
-- (ADR 0003) - see reservation_token below for how complete()/release() are
-- fenced against acting on a reservation that was superseded this way.

CREATE TABLE idempotency_record (
    -- Scope / reservation (endpoint + principal + key value)
    http_method      VARCHAR(10)   NOT NULL,
    path             VARCHAR(512)  NOT NULL,
    principal        VARCHAR(255)  NOT NULL DEFAULT '',   -- '' = unauthenticated route
    idempotency_key  VARCHAR(255)  NOT NULL,              -- opaque value

    -- Collision detection
    -- VARCHAR, not CHAR: Postgres space-pads CHAR(n) values on read, which
    -- corrupts any fingerprint shorter than 64 bytes (SHA-256 hex is always
    -- exactly 64 - a fixed test fixture like "fp" is not).
    fingerprint      VARCHAR(64)   NOT NULL,              -- SHA-256 hex of method+path+normalized body

    -- Fences complete()/release() to the exact reservation attempt that
    -- issued them (ADR 0003): a caller whose reservation was superseded by a
    -- fresh one (stale expires_at) can't act on a record it no longer owns.
    reservation_token VARCHAR(36) NOT NULL,

    -- Cached response (2xx only). Under ADR 0003 a committed row always has
    -- response_status set (it commits together with the response UPDATE, in
    -- the same transaction as the reservation) - NULL here only ever appears
    -- transiently, while the owning reservation's transaction is still open.
    -- response_unavailable is instead reached via a NULL response_body on an
    -- otherwise-committed row (oversized response, see ResponseCapture).
    response_status  SMALLINT,
    response_headers JSONB,                               -- denylist-filtered
    response_body    BYTEA,                               -- exact bytes; supports non-JSON/gzip

    -- Expiration (no native TTL; swept by the cleanup job)
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    expires_at       TIMESTAMPTZ   NOT NULL,

    PRIMARY KEY (http_method, path, principal, idempotency_key)
);

CREATE INDEX ix_idempotency_expires_at ON idempotency_record (expires_at);
