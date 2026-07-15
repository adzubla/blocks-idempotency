package io.adzubla.blocks.idempotency.store;

import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;

/**
 * Thrown by an {@link IdempotencyStore} operation when the backing store is
 * unreachable. The {@link IdempotencyEngine} catches
 * this during {@link IdempotencyStore#reserve} and applies the resolved
 * {@code onStoreFailure} posture (see {@code CONTEXT.md} — Store failure).
 */
public class StoreUnavailableException extends RuntimeException {

    public StoreUnavailableException(String message) {
        super(message);
    }

    public StoreUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
