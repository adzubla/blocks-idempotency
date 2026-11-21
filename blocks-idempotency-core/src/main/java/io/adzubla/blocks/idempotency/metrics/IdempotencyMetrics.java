package io.adzubla.blocks.idempotency.metrics;

/**
 * Records the key idempotency outcomes (PRD user story 29): a replayed
 * response, a fingerprint collision (422), a concurrency conflict (409 -
 * REJECT, WAIT-timeout, or key-gone), a fail-open activation (store
 * unavailable, request let through unprotected), a fail-closed activation
 * (store unavailable, request rejected with 503 - the counterpart to
 * fail-open), a completed-but-not-replayable record (409, no {@code
 * Retry-After} - a response over {@code max-body-size}, or no capture filter in
 * the chain), and a skipped duplicate delivery (messaging: nothing to replay, so
 * the duplicate is acked and skipped).
 * The last three aren't in the original PRD list but share the same
 * store-failure-posture / completed-record branches as the four that are.
 *
 * <p>Every method takes {@code route} and {@code handler} - the same identity
 * an {@link io.adzubla.blocks.idempotency.model.EffectiveKey} carries, minus
 * the principal/key value (too high-cardinality for a metric tag) - so an
 * outcome can be attributed to a specific endpoint/listener rather than only
 * the aggregate across the whole application.
 *
 * <p>Implementations may no-op when metrics are disabled ({@code
 * idempotency.metrics.enabled=false}) or no meter registry is available.
 */
public interface IdempotencyMetrics {

    /** A completed record was replayed ({@code outcome=replay}). */
    void recordReplay(String route, String handler);

    /** The key was reused with a different payload ({@code outcome=collision}). */
    void recordCollision(String route, String handler);

    /** A concurrent duplicate found the key in progress and was rejected or timed out ({@code outcome=concurrency}). */
    void recordConcurrency(String route, String handler);

    /** The store was unavailable and the call proceeded unprotected ({@code outcome=fail_open}). */
    void recordFailOpen(String route, String handler);

    /** The store was unavailable and the call was refused ({@code outcome=fail_closed}). */
    void recordFailClosed(String route, String handler);

    /** A completed record had no replayable response ({@code outcome=response_unavailable}). */
    void recordResponseUnavailable(String route, String handler);

    /**
     * A duplicate delivery of an already-processed key was skipped (the messaging
     * counterpart of a replay: there is no response to replay). A routine outcome,
     * not an error ({@code outcome=duplicate_skipped}).
     */
    void recordDuplicateSkipped(String route, String handler);
}
