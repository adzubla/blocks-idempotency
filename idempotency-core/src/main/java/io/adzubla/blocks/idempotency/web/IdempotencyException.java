package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.engine.RejectReason;
import org.springframework.http.HttpStatus;

/**
 * Base type for the exceptions {@link IdempotencyInterceptor} throws instead of
 * writing the response directly. Thrown from {@code preHandle}, which runs
 * inside {@code DispatcherServlet}'s normal dispatch, so it reaches Spring's
 * {@code HandlerExceptionResolver} chain exactly like an exception thrown from
 * a handler method - {@link IdempotencyExceptionHandler} is the library's own
 * fallback {@code @ControllerAdvice} for these, but an application can declare
 * an {@code @ExceptionHandler} for any of these types (or this common
 * supertype) in its own {@code @ControllerAdvice} to override the response,
 * e.g. to wrap it in its standard error-body envelope.
 *
 * <p>Every subtype carries a {@link #reason()}: {@link IdempotencyExceptionHandler}
 * writes it to {@code Idempotency-Reject-Reason} uniformly across all non-2xx
 * idempotency responses (see ADR 0002).
 */
public sealed abstract class IdempotencyException extends RuntimeException
        permits IdempotencyKeyRequiredException, IdempotencyKeyInvalidException, IdempotencyCollisionException,
                IdempotencyConflictException, IdempotencyResponseUnavailableException, IdempotencyFailClosedException {

    private final HttpStatus status;
    private final RejectReason reason;

    protected IdempotencyException(HttpStatus status, RejectReason reason, String message) {
        super(message);
        this.status = status;
        this.reason = reason;
    }

    /** The status {@link IdempotencyExceptionHandler} falls back to when no application handler overrides this exception. */
    public HttpStatus status() {
        return status;
    }

    /** The value {@link IdempotencyExceptionHandler} writes to {@code Idempotency-Reject-Reason}. */
    public RejectReason reason() {
        return reason;
    }
}
