package io.adzubla.blocks.idempotency.engine;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;

import java.time.Duration;

/**
 * Outcome of {@link IdempotencyEngine#before}. Proceed to execute the handler,
 * replay a completed record, reject a fingerprint mismatch, reject or wait on
 * a concurrent in-progress duplicate, apply the store-failure posture, or
 * report a completed-but-not-replayable record.
 */
public sealed interface EngineDecision {

    /**
     * The key was freshly reserved; the caller should execute the handler,
     * then echo {@code fenceToken} back to {@link IdempotencyEngine#complete}
     * / {@link IdempotencyEngine#release} so a reservation that has since
     * expired and been superseded can't be acted on.
     */
    record Proceed(EffectiveKey key, String fenceToken) implements EngineDecision {
    }

    /**
     * The store is unavailable and the resolved posture is {@code
     * onStoreFailure=OPEN}; the caller should execute the handler unprotected
     * - no reservation exists, so nothing can be completed or released either.
     */
    record ProceedUnprotected() implements EngineDecision {
    }

    /**
     * The store is unavailable and the resolved posture is {@code
     * onStoreFailure=CLOSED}; the caller should reject with 503, terminal,
     * without executing.
     */
    record FailClosed() implements EngineDecision {
    }

    /** A completed record already exists; the caller should replay it verbatim. */
    record Replay(CachedResponse response) implements EngineDecision {
    }

    /**
     * The effect completed but its response isn't replayable (crash window,
     * or body over {@code max-body-size} at completion time); the caller
     * should reject with 409, terminal, without executing or replaying, and
     * no {@code Retry-After} - retrying can never succeed.
     */
    record Unavailable() implements EngineDecision {
    }

    /**
     * The key was reused with a different payload (fingerprint mismatch); the
     * caller should reject with 422, terminal, without executing or replaying.
     */
    record Collision() implements EngineDecision {
    }

    /**
     * A concurrent duplicate found the key still in-progress (same
     * fingerprint): immediately under {@code whenInProgress=REJECT}, or after
     * waiting under {@code whenInProgress=WAIT} and the key went away or
     * {@code wait-timeout} elapsed. The caller should reject with 409 +
     * {@code Retry-After: retryAfter}, without executing or replaying.
     */
    record Reject(RejectReason reason, Duration retryAfter) implements EngineDecision {
    }
}
