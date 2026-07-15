package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.engine.RejectReason;
import org.springframework.http.HttpStatus;

/** The key was reused with a different payload (method+path+body fingerprint mismatch). Terminal - never re-executes. */
public final class IdempotencyCollisionException extends IdempotencyException {

    public IdempotencyCollisionException() {
        super(HttpStatus.UNPROCESSABLE_CONTENT, RejectReason.COLLISION, "Idempotency key reused with a different request payload");
    }
}
