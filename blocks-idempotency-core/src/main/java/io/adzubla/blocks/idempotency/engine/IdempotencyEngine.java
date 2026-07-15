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
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

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
            log.debug("Idempotency key reserved: {} {} key={}", key.method(), key.path(), key.value());
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
     * Polls the key (base {@link #pollInterval} + up to {@link #pollJitter},
     * ADR 0002's polling model) until it completes, disappears, or {@code
     * waitTimeout} elapses. Each sleep is capped to the remaining budget, so
     * the wait never overshoots {@code waitTimeout}. Thread-holding by design
     * (ADR 0002) - the waiter never executes the effect itself (no
     * self-promotion).
     */
    private EngineDecision waitForCompletion(EffectiveKey key, Duration lockTtl, Duration waitTimeout, OnStoreFailure onStoreFailure) {
        Instant deadline = Instant.now().plus(waitTimeout);
        while (true) {
            Duration remaining = Duration.between(Instant.now(), deadline);
            if (remaining.isZero() || remaining.isNegative()) {
                return reject(key, RejectReason.TIMEOUT, lockTtl);
            }
            sleepWithJitter(remaining);

            Optional<IdempotencyRecord> current;
            try {
                current = store.find(key);
            } catch (StoreUnavailableException e) {
                return applyStoreFailurePosture(key, onStoreFailure, e);
            }
            if (current.isEmpty()) {
                // The primary failed (error releases the key) or its lock
                // expired (crash) - either way, the key is gone.
                return reject(key, RejectReason.RELEASED, lockTtl);
            }
            IdempotencyRecord record = current.get();
            if (record.isCompleted()) {
                return decisionForCompleted(key, record.response());
            }
            // Still in-progress - loop back, remaining budget re-checked above.
        }
    }

    private EngineDecision decisionForCompleted(EffectiveKey key, CachedResponse response) {
        if (response != null && response.hasBody()) {
            log.debug("Replaying cached response for idempotency key: {} {} key={}", key.method(), key.path(), key.value());
            metrics.recordReplay();
            return new EngineDecision.Replay(response);
        }
        log.debug("Idempotency response unavailable (not replayable): {} {} key={}", key.method(), key.path(), key.value());
        return new EngineDecision.Unavailable();
    }

    private EngineDecision collision(EffectiveKey key) {
        log.debug("Idempotency collision (fingerprint mismatch): {} {} key={}", key.method(), key.path(), key.value());
        metrics.recordCollision();
        return new EngineDecision.Collision();
    }

    private EngineDecision reject(EffectiveKey key, RejectReason reason, Duration retryAfter) {
        log.debug("Rejecting concurrent duplicate: {} {} key={} reason={}", key.method(), key.path(), key.value(), reason.wireValue());
        metrics.recordConcurrency();
        return new EngineDecision.Reject(reason, retryAfter);
    }

    private EngineDecision applyStoreFailurePosture(EffectiveKey key, OnStoreFailure onStoreFailure, StoreUnavailableException cause) {
        if (onStoreFailure == OnStoreFailure.CLOSED) {
            log.warn("Idempotency store unavailable for {} {} - failing closed (503)", key.method(), key.path(), cause);
            return new EngineDecision.FailClosed();
        }
        log.warn("Idempotency store unavailable for {} {} - failing open (unprotected)", key.method(), key.path(), cause);
        metrics.recordFailOpen();
        return new EngineDecision.ProceedUnprotected();
    }

    /** Package-private so tests can verify jitter directly without a full WAIT loop. */
    void sleepWithJitter() {
        sleepWithJitter(pollInterval.plus(pollJitter).plusMillis(1));
    }

    /** As {@link #sleepWithJitter()}, but never sleeps past {@code cap} - keeps the WAIT loop within {@code waitTimeout}. */
    private void sleepWithJitter(Duration cap) {
        long jitterMillis = pollJitter.toMillis() > 0 ? ThreadLocalRandom.current().nextLong(pollJitter.toMillis() + 1) : 0;
        long delayMillis = Math.min(pollInterval.toMillis() + jitterMillis, cap.toMillis());
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the idempotency key to complete", e);
        }
    }

    public void complete(EffectiveKey key, String fenceToken, CachedResponse response, Duration responseTtl) {
        try {
            store.complete(key, fenceToken, response, responseTtl);
            log.debug("Idempotency key completed: {} {} key={} status={}", key.method(), key.path(), key.value(), response.status());
        } catch (StoreUnavailableException e) {
            // The effect already ran; there's no response left to protect by
            // applying a posture here (that's only meaningful before
            // execution, in before()). The record simply won't be cached
            // until the store recovers - swallow rather than fail the
            // already-completed request out from under the caller.
            log.warn("Idempotency store unavailable while completing {} {} key={} - response won't be cached",
                    key.method(), key.path(), key.value(), e);
        }
    }

    /** Releases the reservation (non-2xx or thrown exception), so a genuine retry re-executes. */
    public void release(EffectiveKey key, String fenceToken) {
        try {
            store.release(key, fenceToken);
            log.debug("Idempotency key released: {} {} key={}", key.method(), key.path(), key.value());
        } catch (StoreUnavailableException e) {
            // Same rationale as complete(): the effect already ran (or
            // failed) - nothing left to protect by escalating here.
            log.warn("Idempotency store unavailable while releasing {} {} key={}", key.method(), key.path(), key.value(), e);
        }
    }
}
