package io.adzubla.blocks.idempotency.metrics;

/**
 * Records the key idempotency outcomes (PRD user story 29): a replayed
 * response, a fingerprint collision (422), a concurrency conflict (409 -
 * REJECT, WAIT-timeout, or key-gone), a fail-open activation (store
 * unavailable, request let through unprotected), a fail-closed activation
 * (store unavailable, request rejected with 503 - the counterpart to
 * fail-open), and a completed-but-not-replayable record (409, no {@code
 * Retry-After} - crash window or a response over {@code max-body-size}).
 * The last two aren't in the original PRD list but share the same
 * store-failure-posture / completed-record branches as the four that are.
 * Implementations may no-op when metrics are disabled ({@code
 * idempotency.metrics.enabled=false}) or no meter registry is available.
 */
public interface IdempotencyMetrics {

    void recordReplay();

    void recordCollision();

    void recordConcurrency();

    void recordFailOpen();

    void recordFailClosed();

    void recordResponseUnavailable();
}
