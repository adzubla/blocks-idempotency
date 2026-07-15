package io.adzubla.blocks.idempotency.metrics;

/**
 * Records the key idempotency outcomes (PRD user story 29): a replayed
 * response, a fingerprint collision (422), a concurrency conflict (409 -
 * REJECT, WAIT-timeout, or key-gone), and a fail-open activation (store
 * unavailable, request let through unprotected). Implementations may no-op
 * when metrics are disabled ({@code idempotency.metrics.enabled=false}) or
 * no meter registry is available.
 */
public interface IdempotencyMetrics {

    void recordReplay();

    void recordCollision();

    void recordConcurrency();

    void recordFailOpen();
}
