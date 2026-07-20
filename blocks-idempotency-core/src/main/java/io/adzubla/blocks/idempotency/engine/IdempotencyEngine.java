package io.adzubla.blocks.idempotency.engine;

import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.metrics.IdempotencyMetrics;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.IdempotencyRecord;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.StoreUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

/**
 * Orchestrates the idempotency flow over an {@link IdempotencyStore}:
 * reserve, branch on key lifecycle, execute or replay, complete on 2xx.
 * Framework-free; depends only on the store and model abstractions.
 *
 * <p>Slice 001-004, 007-011 scope: fresh-reservation, completed-replay,
 * fingerprint-mismatch (collision), concurrent in-progress (reject or, under
 * {@code whenInProgress=WAIT}, poll-and-wait), error-release,
 * store-failure-posture, TTL-expiry, completed-but-not-replayable ({@code
 * response_unavailable}), and outcome-metrics branches.
 */
public class IdempotencyEngine {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyEngine.class);

    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(100);
    private static final Duration DEFAULT_POLL_JITTER = Duration.ofMillis(50);

    private final IdempotencyStore store;
    private final Duration pollInterval;
    private final Duration pollJitter;
    private final IdempotencyMetrics metrics;

    public IdempotencyEngine(IdempotencyStore store) {
        this(store, DEFAULT_POLL_INTERVAL, DEFAULT_POLL_JITTER, NoOpIdempotencyMetrics.INSTANCE);
    }

    public IdempotencyEngine(IdempotencyStore store, IdempotencyMetrics metrics) {
        this(store, DEFAULT_POLL_INTERVAL, DEFAULT_POLL_JITTER, metrics);
    }

    /**
     * @param pollInterval base delay between WAIT-mode polls (ADR 0002: "~100ms")
     * @param pollJitter   extra random delay added to each poll, up to this much (ADR 0002: "with jitter")
     */
    public IdempotencyEngine(IdempotencyStore store, Duration pollInterval, Duration pollJitter) {
        this(store, pollInterval, pollJitter, NoOpIdempotencyMetrics.INSTANCE);
    }

    public IdempotencyEngine(IdempotencyStore store, Duration pollInterval, Duration pollJitter, IdempotencyMetrics metrics) {
        this.store = store;
        this.pollInterval = pollInterval;
        this.pollJitter = pollJitter;
        this.metrics = metrics;
    }

    public EngineDecision before(EffectiveKey key, String fingerprint, Duration lockTtl, OnStoreFailure onStoreFailure,
            WhenInProgress whenInProgress, Duration waitTimeout) {
        ReservationResult result;
        try {
            result = store.reserve(key, fingerprint, lockTtl);
        } catch (StoreUnavailableException e) {
            return applyStoreFailurePosture(key, onStoreFailure, e);
        }
        if (result.outcome() == ReservationResult.Outcome.RESERVED) {
            log.debug("Idempotency key reserved: {} {} key={}", key.route(), key.handler(), key.value());
            return new EngineDecision.Proceed(key, result.fenceToken().orElseThrow());
        }

        IdempotencyRecord existing = result.existing().orElseThrow();
        if (!existing.fingerprint().equals(fingerprint)) {
            return collision(key);
        }
        if (existing.isCompleted()) {
            return decisionForCompleted(key, existing.response());
        }

        // IN_PROGRESS with the same fingerprint: a concurrent duplicate.
        if (whenInProgress == WhenInProgress.WAIT) {
            return waitForCompletion(key, lockTtl, waitTimeout, onStoreFailure);
        }
        // Default whenInProgress=REJECT - immediate 409, no thread held.
        return reject(key, RejectReason.IN_PROGRESS, lockTtl);
    }

    /**
     * Delegates to {@link IdempotencyStore#await} (ADR 0002's WAIT mode) and
     * translates its outcome. Thread-holding by design (ADR 0002) - the
     * waiter never executes the effect itself (no self-promotion).
     */
    private EngineDecision waitForCompletion(EffectiveKey key, Duration lockTtl, Duration waitTimeout, OnStoreFailure onStoreFailure) {
        Optional<IdempotencyRecord> result;
        try {
            result = store.await(key, waitTimeout, pollInterval, pollJitter);
        } catch (StoreUnavailableException e) {
            return applyStoreFailurePosture(key, onStoreFailure, e);
        }
        if (result.isEmpty()) {
            // The primary failed (error releases the key) or its lock
            // expired (crash) - either way, the key is gone.
            return reject(key, RejectReason.RELEASED, lockTtl);
        }
        IdempotencyRecord record = result.get();
        if (record.isCompleted()) {
            return decisionForCompleted(key, record.response());
        }
        // Still in-progress once waitTimeout ran out - give up.
        return reject(key, RejectReason.TIMEOUT, lockTtl);
    }

    /**
     * {@code Unavailable} means different things to different callers: for
     * HTTP it's a terminal error (crash window, or a body over {@code
     * max-body-size} - see {@link CachedResponse}), surfaced to the client as
     * a non-retryable 409. A caller that always completes a record with
     * {@link CachedResponse#empty()} (no response to cache at all - e.g. a
     * future messaging adapter, per ADR 0004) will see every completed
     * duplicate resolve to {@code Unavailable} here, never {@code Replay} -
     * that's its routine, expected outcome for a duplicate delivery, not an
     * error condition to propagate.
     */
    private EngineDecision decisionForCompleted(EffectiveKey key, CachedResponse response) {
        if (response != null && response.hasBody()) {
            log.debug("Replaying cached response for idempotency key: {} {} key={}", key.route(), key.handler(), key.value());
            metrics.recordReplay();
            return new EngineDecision.Replay(response);
        }
        log.debug("Idempotency response unavailable (not replayable): {} {} key={}", key.route(), key.handler(), key.value());
        return new EngineDecision.Unavailable();
    }

    private EngineDecision collision(EffectiveKey key) {
        log.debug("Idempotency collision (fingerprint mismatch): {} {} key={}", key.route(), key.handler(), key.value());
        metrics.recordCollision();
        return new EngineDecision.Collision();
    }

    private EngineDecision reject(EffectiveKey key, RejectReason reason, Duration retryAfter) {
        log.debug("Rejecting concurrent duplicate: {} {} key={} reason={}", key.route(), key.handler(), key.value(), reason.wireValue());
        metrics.recordConcurrency();
        return new EngineDecision.Reject(reason, retryAfter);
    }

    private EngineDecision applyStoreFailurePosture(EffectiveKey key, OnStoreFailure onStoreFailure, StoreUnavailableException cause) {
        if (onStoreFailure == OnStoreFailure.CLOSED) {
            log.warn("Idempotency store unavailable for {} {} - failing closed (503)", key.route(), key.handler(), cause);
            return new EngineDecision.FailClosed();
        }
        log.warn("Idempotency store unavailable for {} {} - failing open (unprotected)", key.route(), key.handler(), cause);
        metrics.recordFailOpen();
        return new EngineDecision.ProceedUnprotected();
    }

    public void complete(EffectiveKey key, String fenceToken, CachedResponse response, Duration responseTtl) {
        try {
            store.complete(key, fenceToken, response, responseTtl);
            log.debug("Idempotency key completed: {} {} key={} status={}", key.route(), key.handler(), key.value(), response.status());
        } catch (StoreUnavailableException e) {
            // The effect already ran; there's no response left to protect by
            // applying a posture here (that's only meaningful before
            // execution, in before()). The record simply won't be cached
            // until the store recovers - swallow rather than fail the
            // already-completed request out from under the caller.
            log.warn("Idempotency store unavailable while completing {} {} key={} - response won't be cached",
                    key.route(), key.handler(), key.value(), e);
        }
    }

    /** Releases the reservation (non-2xx or thrown exception), so a genuine retry re-executes. */
    public void release(EffectiveKey key, String fenceToken) {
        try {
            store.release(key, fenceToken);
            log.debug("Idempotency key released: {} {} key={}", key.route(), key.handler(), key.value());
        } catch (StoreUnavailableException e) {
            // Same rationale as complete(): the effect already ran (or
            // failed) - nothing left to protect by escalating here.
            log.warn("Idempotency store unavailable while releasing {} {} key={}", key.route(), key.handler(), key.value(), e);
        }
    }
}
