package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.engine.RejectReason;
import org.springframework.http.HttpStatus;

/** {@code keyRequired=true} (the default) but the header/body-field key was absent from the request. */
public final class IdempotencyKeyRequiredException extends IdempotencyException {

    public IdempotencyKeyRequiredException() {
        super(HttpStatus.BAD_REQUEST, RejectReason.KEY_REQUIRED, "Idempotency key required but not present in the request");
    }
}
