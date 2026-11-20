package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.engine.RejectReason;
import org.springframework.http.HttpStatus;

/**
 * The request body on an {@code @Idempotent} endpoint exceeds {@code
 * idempotency.max-request-body-size}, so it isn't buffered for the fingerprint or body-field key.
 * The body is rejected while buffering, never read in full.
 */
public final class IdempotencyRequestTooLargeException extends IdempotencyException {

    public IdempotencyRequestTooLargeException() {
        super(HttpStatus.CONTENT_TOO_LARGE, RejectReason.REQUEST_TOO_LARGE,
                "Request body exceeds the configured idempotency.max-request-body-size");
    }
}
