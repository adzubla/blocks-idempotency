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
 * <p>Branches handled: fresh reservation, completed replay, fingerprint
 * mismatch (collision), concurrent in-progress (reject or, under {@code
 * whenInProgress=WAIT}, poll-and-wait), error release, store-failure posture,
 * TTL expiry, completed-but-not-replayable ({@code response_unavailable}),
 * completed-with-no-response duplicate ({@code AlreadyProcessed}, messaging),
 * and outcome metrics.
 */
public class IdempotencyEngine {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyEngine.class);

    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(100);
    private static final Duration DEFAULT_POLL_JITTER = Duration.ofMillis(50);

    private final IdempotencyStore store;
    private final Duration pollInterval;
    private final Duration pollJitter;
    private final IdempotencyMetrics metrics;

    /** Engine with the default WAIT polling (100 ms plus up to 50 ms jitter) and no metrics. */
    public IdempotencyEngine(IdempotencyStore store) {
        this(store, DEFAULT_POLL_INTERVAL, DEFAULT_POLL_JITTER, NoOpIdempotencyMetrics.INSTANCE);
    }

    /** Engine with the default WAIT polling (100 ms plus up to 50 ms jitter) that records outcomes to {@code metrics}. */
    public IdempotencyEngine(IdempotencyStore store, IdempotencyMetrics metrics) {
        this(store, DEFAULT_POLL_INTERVAL, DEFAULT_POLL_JITTER, metrics);
    }

    /**
     * Engine with custom WAIT polling and no metrics.
     *
     * @param store        the store holding reservations and cached responses
     * @param pollInterval base delay between WAIT-mode polls (ADR 0002: "~100ms")
     * @param pollJitter   extra random delay added to each poll, up to this much (ADR 0002: "with jitter")
     */
    public IdempotencyEngine(IdempotencyStore store, Duration pollInterval, Duration pollJitter) {
        this(store, pollInterval, pollJitter, NoOpIdempotencyMetrics.INSTANCE);
    }

    /**
     * Engine with custom WAIT polling that records outcomes to {@code metrics}.
     *
     * @param store        the store holding reservations and cached responses
     * @param pollInterval base delay between WAIT-mode polls
     * @param pollJitter   extra random delay added to each poll, up to this much
     * @param metrics      receives one outcome per decision made by {@link #before}
     */
    public IdempotencyEngine(IdempotencyStore store, Duration pollInterval, Duration pollJitter, IdempotencyMetrics metrics) {
        this.store = store;
        this.pollInterval = pollInterval;
        this.pollJitter = pollJitter;
        this.metrics = metrics;
    }

    /** @return whether this engine's store commits the effect in {@code complete} (see {@link IdempotencyStore#commitsEffect()}) */
    public boolean commitsEffect() {
        return store.commitsEffect();
    }

    /**
     * Decides what to do with a request or delivery before the effect runs: reserves
     * the key and, if it is already held, branches on the existing record (collision,
     * replay, already processed, response unavailable, or concurrent in-progress,
     * which is rejected or awaited per {@code whenInProgress}). A store outage is
     * resolved here by {@code onStoreFailure}, so this method does not throw for it.
     *
     * @param key             the effective key (scope plus key value)
     * @param fingerprint     digest of the request used to detect key reuse with a different payload
     * @param lockTtl         how long the reservation stays valid while the effect runs
     * @param onStoreFailure  posture applied if the store is unavailable
     * @param whenInProgress  whether a concurrent duplicate is rejected or waits
     * @param waitTimeout     longest a {@code WAIT} duplicate holds its thread
     * @return the decision the caller must act on; {@code Proceed} carries the fence
     *         token to pass back to {@link #complete} or {@link #release}
     */
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
        // Still in-progress once waitTimeout ran out, or the wait was
        // interrupted (the interrupt status stays set for the caller) - give up.
        if (Thread.currentThread().isInterrupted()) {
            log.debug("WAIT interrupted before the key resolved: {} {} key={}", key.route(), key.handler(), key.value());
        }
        return reject(key, RejectReason.TIMEOUT, lockTtl);
    }

    /**
     * Maps a completed record to a decision: {@code Replay} when it holds a
     * body; {@code AlreadyProcessed} when it was completed with {@link
     * CachedResponse#empty()} (a caller with no response to cache - the
     * Kafka/RabbitMQ/JMS messaging advice, per ADR 0004), the routine "skip this
     * duplicate" outcome counted as {@code duplicate_skipped}; otherwise
     * {@code Unavailable}, the terminal error for an HTTP response that should
     * have been replayable but wasn't (a body over {@code max-body-size}, or the
     * capture filter missing from the chain - see {@link CachedResponse}),
     * surfaced as a non-retryable 409.
     */
    private EngineDecision decisionForCompleted(EffectiveKey key, CachedResponse response) {
        if (response != null && response.hasBody()) {
            log.debug("Replaying cached response for idempotency key: {} {} key={}", key.route(), key.handler(), key.value());
            metrics.recordReplay(key.route(), key.handler());
            return new EngineDecision.Replay(response);
        }
        if (response != null && response.isEmpty()) {
            log.debug("Duplicate of an already-processed key, skipping: {} {} key={}", key.route(), key.handler(), key.value());
            metrics.recordDuplicateSkipped(key.route(), key.handler());
            return new EngineDecision.AlreadyProcessed();
        }
        log.debug("Idempotency response unavailable (not replayable): {} {} key={}", key.route(), key.handler(), key.value());
        metrics.recordResponseUnavailable(key.route(), key.handler());
        return new EngineDecision.Unavailable();
    }

    private EngineDecision collision(EffectiveKey key) {
        log.debug("Idempotency collision (fingerprint mismatch): {} {} key={}", key.route(), key.handler(), key.value());
        metrics.recordCollision(key.route(), key.handler());
        return new EngineDecision.Collision();
    }

    private EngineDecision reject(EffectiveKey key, RejectReason reason, Duration retryAfter) {
        log.debug("Rejecting concurrent duplicate: {} {} key={} reason={}", key.route(), key.handler(), key.value(), reason.wireValue());
        metrics.recordConcurrency(key.route(), key.handler());
        return new EngineDecision.Reject(reason, retryAfter);
    }

    private EngineDecision applyStoreFailurePosture(EffectiveKey key, OnStoreFailure onStoreFailure, StoreUnavailableException cause) {
        if (onStoreFailure == OnStoreFailure.CLOSED) {
            log.warn("Idempotency store unavailable for {} {} - failing closed (503)", key.route(), key.handler(), cause);
            metrics.recordFailClosed(key.route(), key.handler());
            return new EngineDecision.FailClosed();
        }
        log.warn("Idempotency store unavailable for {} {} - failing open (unprotected)", key.route(), key.handler(), cause);
        metrics.recordFailOpen(key.route(), key.handler());
        return new EngineDecision.ProceedUnprotected();
    }

    /**
     * Completes the record. A {@link StoreUnavailableException} is swallowed
     * (the effect ran, only its caching is lost); an {@link
     * io.adzubla.blocks.idempotency.store.EffectRolledBackException} is not -
     * the store rolled the effect back, so the caller must not report success.
     *
     * @param key         the key returned in the {@code Proceed} decision
     * @param fenceToken  the token returned in the {@code Proceed} decision
     * @param response    the captured response, or {@link CachedResponse#empty()} when there is none
     * @param responseTtl how long the completed record is kept
     */
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

    /**
     * Releases the reservation (non-2xx or thrown exception), so a genuine retry
     * re-executes. A {@link StoreUnavailableException} is swallowed: the reservation
     * then simply expires after its lock TTL.
     *
     * @param key        the key returned in the {@code Proceed} decision
     * @param fenceToken the token returned in the {@code Proceed} decision
     */
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
