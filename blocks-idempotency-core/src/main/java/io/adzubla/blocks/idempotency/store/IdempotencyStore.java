package io.adzubla.blocks.idempotency.store;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.IdempotencyRecord;
import io.adzubla.blocks.idempotency.model.ReservationResult;

import java.time.Duration;
import java.util.Optional;

/**
 * Persists the reservation and cached response of an {@link EffectiveKey}. The
 * central SPI of the library (see ADR 0001). Implementations differ in their
 * guarantees: {@code redis} is best-effort; {@code postgres} is exactly-once on the
 * effect when it writes to the same database/transaction.
 */
public interface IdempotencyStore {

    /**
     * Atomically reserve the key, or return the existing record. On first call it
     * registers the key {@code IN_PROGRESS} with the given fingerprint and lock TTL,
     * and the result carries a fence token for the caller to echo back to
     * {@link #complete}/{@link #release}.
     *
     * @param key         the effective key to reserve
     * @param fingerprint digest of the request, stored to detect key reuse with a different payload
     * @param lockTtl     how long the reservation stays valid before it can be superseded
     * @return {@code RESERVED} with a fence token, or {@code EXISTS} with the current record
     * @throws StoreUnavailableException if the store cannot be reached
     */
    ReservationResult reserve(EffectiveKey key, String fingerprint, Duration lockTtl);

    /**
     * Whether a reservation may be completed or released on a different thread than the one that
     * made it, as an asynchronous HTTP handler ({@code CompletableFuture}, {@code DeferredResult},
     * {@code Callable}, streaming bodies) requires. A store whose reservation is bound to the
     * reserving thread (Postgres: the reservation is that thread's open transaction) returns
     * {@code false}, and startup validation rejects asynchronous {@code @Idempotent} handlers on it.
     */
    default boolean supportsAsyncHandlers() {
        return true;
    }

    /**
     * Whether {@link #complete} commits the effect itself, so it can still fail with
     * {@link EffectRolledBackException} after the handler has returned. When it can, the
     * HTTP response must stay buffered until {@code complete} returns, whatever its size,
     * so that a rolled-back effect can be reported as a {@code 500} rather than the 2xx
     * the handler produced. Otherwise a response too large to cache is streamed to the
     * client instead of being held in memory. {@code false} by default; {@code true} for
     * Postgres.
     *
     * @return whether {@code complete} commits the effect
     */
    default boolean commitsEffect() {
        return false;
    }

    /**
     * Look up the current record, if any.
     *
     * @param key the effective key
     * @return the record, or empty if none exists or it has expired
     * @throws StoreUnavailableException if the store cannot be reached
     */
    Optional<IdempotencyRecord> find(EffectiveKey key);

    /**
     * Mark the key completed with the captured response and extend to the response TTL.
     * A no-op if {@code fenceToken} no longer matches the current reservation (it
     * expired and was superseded) - see {@link ReservationResult}.
     *
     * @param key         the effective key
     * @param fenceToken  the token from {@link #reserve}
     * @param response    the captured response, or {@link CachedResponse#empty()} when there is none
     * @param responseTtl how long the completed record is kept
     * @throws EffectRolledBackException if this store owns the effect's
     *         transaction and it could not be committed - the effect did not happen
     */
    void complete(EffectiveKey key, String fenceToken, CachedResponse response, Duration responseTtl);

    /**
     * Release the reservation (e.g. after a non-2xx), so a genuine retry can proceed.
     * A no-op if {@code fenceToken} no longer matches the current reservation - see
     * {@link ReservationResult}.
     *
     * @param key        the effective key
     * @param fenceToken the token from {@link #reserve}
     */
    void release(EffectiveKey key, String fenceToken);

    /**
     * Wait for an in-progress key to reach a terminal state (ADR 0002 WAIT mode):
     * returns the record once it's completed, {@link Optional#empty()} if it
     * disappears (released, or its lock expired), or the still-in-progress
     * record if {@code waitTimeout} elapses first - the caller tells "timed
     * out" from "released" apart by whether the {@link Optional} is present.
     *
     * <p>The default polls via {@link #find} at {@code pollInterval} (plus up
     * to {@code pollJitter}) - the core model from ADR 0002 - checking once
     * before the first sleep, so an already-terminal key returns immediately.
     * A thread interrupt ends the default's wait early, like an elapsed {@code
     * waitTimeout}: the last record observed is returned (still in progress),
     * and the thread's interrupt status is left set for the caller. Stores
     * with a native blocking primitive (Postgres's row lock) may override
     * with their own mechanism instead, ignoring {@code
     * pollInterval}/{@code pollJitter}; that wait is a blocking JDBC call
     * bounded by the remaining {@code waitTimeout}, which an interrupt does
     * not cut short.
     *
     * <p>The deadline is measured on the monotonic clock ({@link
     * System#nanoTime()}), not the wall clock or a store's injectable {@code
     * Clock}: {@code waitTimeout} bounds real time spent sleeping, so it must
     * be measured on the same time base as the sleep. A wall clock that steps
     * would cut the wait short or stretch it; a fake {@code Clock} that never
     * advances would make it never end.
     *
     * @param key         the effective key to wait on
     * @param waitTimeout longest to wait for a terminal state
     * @param pollInterval base delay between polls (ignored by stores that block natively)
     * @param pollJitter  extra random delay added to each poll, up to this much
     * @return the completed record, the still-in-progress record on timeout, or empty if released or expired
     */
    default Optional<IdempotencyRecord> await(EffectiveKey key, Duration waitTimeout, Duration pollInterval, Duration pollJitter) {
        long deadline = System.nanoTime() + waitTimeout.toNanos();
        Optional<IdempotencyRecord> current = find(key);
        while (current.isPresent() && !current.get().isCompleted()) {
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                break;
            }
            if (!PollJitter.sleep(pollInterval, pollJitter, Duration.ofNanos(remainingNanos))) {
                // Interrupted: stop waiting without touching the store again
                // (a client may refuse I/O on an interrupted thread).
                break;
            }
            current = find(key);
        }
        return current;
    }
}
