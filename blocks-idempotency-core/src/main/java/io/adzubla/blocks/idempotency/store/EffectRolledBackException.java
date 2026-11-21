package io.adzubla.blocks.idempotency.store;

/**
 * Thrown by {@link IdempotencyStore#complete} when the store owns the
 * transaction the effect ran in (e.g. {@code postgres}, ADR 0003) and that
 * transaction could not be committed - the handler returned successfully, but
 * its effect was rolled back along with the reservation.
 *
 * <p>Deliberately not a {@link StoreUnavailableException}: that one means
 * "the effect already ran, only the caching was lost" and is swallowed by
 * {@code IdempotencyEngine#complete}. This one means the opposite - nothing
 * was persisted - so it propagates to the transport adapter, which must not
 * report the handler's success to the caller.
 */
public class EffectRolledBackException extends RuntimeException {

    /**
     * @param message description of the failed commit
     * @param cause   the underlying commit failure
     */
    public EffectRolledBackException(String message, Throwable cause) {
        super(message, cause);
    }
}
