package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.engine.RejectReason;
import org.springframework.http.HttpStatus;

/** The raw key value exceeds {@code idempotency.key.max-length} or contains characters outside the allowed charset. */
public final class IdempotencyKeyInvalidException extends IdempotencyException {

    public IdempotencyKeyInvalidException() {
        super(HttpStatus.BAD_REQUEST, RejectReason.KEY_INVALID, "Idempotency key value violates the configured size/charset limit");
    }
}
