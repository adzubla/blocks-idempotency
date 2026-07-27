package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.engine.RejectReason;
import org.springframework.http.HttpStatus;

import java.time.Duration;

/**
 * A concurrent duplicate found the key in-progress ({@code whenInProgress=REJECT}),
 * a {@code WAIT} caller timed out, or the primary released the key mid-wait. All
 * three are retryable with the same key - carries the {@code Retry-After} the
 * caller should honor.
 */
public final class IdempotencyConflictException extends IdempotencyException {

    private final Duration retryAfter;

    public IdempotencyConflictException(RejectReason reason, Duration retryAfter) {
        super(HttpStatus.CONFLICT, reason, "Idempotency conflict: " + reason.wireValue());
        this.retryAfter = retryAfter;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
