package io.adzubla.blocks.idempotency.store;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.IdempotencyRecord;
import io.adzubla.blocks.idempotency.model.ReservationResult;

import java.time.Duration;
import java.time.Instant;
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
     */
    ReservationResult reserve(EffectiveKey key, String fingerprint, Duration lockTtl);

    /** Look up the current record, if any. */
    Optional<IdempotencyRecord> find(EffectiveKey key);

    /**
     * Mark the key completed with the captured response and extend to the response TTL.
     * A no-op if {@code fenceToken} no longer matches the current reservation (it
     * expired and was superseded) - see {@link ReservationResult}.
     */
    void complete(EffectiveKey key, String fenceToken, CachedResponse response, Duration responseTtl);

    /**
     * Release the reservation (e.g. after a non-2xx), so a genuine retry can proceed.
     * A no-op if {@code fenceToken} no longer matches the current reservation - see
     * {@link ReservationResult}.
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
     * to {@code pollJitter}) - the core model from ADR 0002. Stores with a
     * native blocking primitive (Postgres's row lock) may override with their
     * own mechanism instead, ignoring {@code pollInterval}/{@code pollJitter}.
     */
    default Optional<IdempotencyRecord> await(EffectiveKey key, Duration waitTimeout, Duration pollInterval, Duration pollJitter) {
        Instant deadline = Instant.now().plus(waitTimeout);
        while (true) {
            Duration remaining = Duration.between(Instant.now(), deadline);
            if (remaining.isZero() || remaining.isNegative()) {
                return find(key);
            }
            PollJitter.sleep(pollInterval, pollJitter, remaining);

            Optional<IdempotencyRecord> current = find(key);
            if (current.isEmpty() || current.get().isCompleted()) {
                return current;
            }
            // Still in-progress - loop back, remaining budget re-checked above.
        }
    }
}
