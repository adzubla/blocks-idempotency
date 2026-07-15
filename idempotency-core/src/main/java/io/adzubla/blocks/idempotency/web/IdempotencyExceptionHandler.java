package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.engine.RejectReason;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Default translation of the library's {@link IdempotencyException}s into the
 * same bare status(+headers)/no-body responses {@link IdempotencyInterceptor}
 * used to write directly before it started throwing them instead. Uniform
 * across every non-2xx idempotency response (see ADR 0002): every one carries
 * {@code Idempotency-Reject-Reason}; only {@link IdempotencyConflictException}
 * additionally carries {@code Retry-After}, since it alone is the "resend with
 * the same key" case.
 *
 * <p>Ordered last ({@link Ordered#LOWEST_PRECEDENCE}) so an application's own
 * {@code @ControllerAdvice} can override any single one of these - {@code
 * ExceptionHandlerExceptionResolver} tries advice beans in {@code @Order}
 * first, falling back to this one only when nothing more specific (or
 * higher-priority) handled the exception. Declaring an {@code
 * @ExceptionHandler} for the same exception type (or {@link
 * IdempotencyException} itself) in application code is enough to take over,
 * e.g. to wrap the error in the application's own JSON error envelope.
 */
@RestControllerAdvice
@Order(Ordered.LOWEST_PRECEDENCE)
public class IdempotencyExceptionHandler {

    @ExceptionHandler(IdempotencyException.class)
    public ResponseEntity<Void> handle(IdempotencyException ex) {
        ResponseEntity.BodyBuilder response = ResponseEntity.status(ex.status())
                .header(RejectReason.HEADER_NAME, ex.reason().wireValue());
        if (ex instanceof IdempotencyConflictException conflict) {
            response.header(HttpHeaders.RETRY_AFTER, String.valueOf(conflict.retryAfter().toSeconds()));
        }
        return response.build();
    }
}
