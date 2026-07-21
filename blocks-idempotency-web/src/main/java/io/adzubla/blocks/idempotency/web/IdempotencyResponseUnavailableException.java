package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.engine.RejectReason;
import org.springframework.http.HttpStatus;

/**
 * The effect completed but its response isn't replayable (crash window, or the
 * body was over {@code max-body-size} at completion time). Terminal - no
 * {@code Retry-After}, retrying can never succeed.
 */
public final class IdempotencyResponseUnavailableException extends IdempotencyException {

    public IdempotencyResponseUnavailableException() {
        super(HttpStatus.CONFLICT, RejectReason.RESPONSE_UNAVAILABLE, "Idempotent response is not replayable");
    }
}
