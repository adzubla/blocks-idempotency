package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.engine.RejectReason;
import org.springframework.http.HttpStatus;

/**
 * The store is unavailable and the resolved posture is {@code onStoreFailure=CLOSED}.
 * Distinct from {@link io.adzubla.blocks.idempotency.store.StoreUnavailableException},
 * which a store implementation throws internally - this is the HTTP-facing
 * decision the engine already made to reject rather than proceed unprotected.
 */
public final class IdempotencyFailClosedException extends IdempotencyException {

    public IdempotencyFailClosedException() {
        super(HttpStatus.SERVICE_UNAVAILABLE, RejectReason.STORE_UNAVAILABLE, "Idempotency store is unavailable");
    }
}
