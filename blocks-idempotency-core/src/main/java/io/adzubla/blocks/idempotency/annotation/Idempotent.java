package io.adzubla.blocks.idempotency.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a controller handler as idempotent.
 *
 * <p>Exactly one of {@link #header()} / {@link #fieldPath()} must be set — validated
 * at startup. All attributes except {@link #keyRequired()} inherit the global default
 * when left at their sentinel ({@code ""} for strings, {@code DEFAULT} for enums).
 *
 * <p>See {@code CONTEXT.md} and {@code docs/adr/} for the full design.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Idempotent {

    /** Suggested value for {@link #header()}; most callers can just use this. */
    String IDEMPOTENCY_KEY_HEADER = "X-Idempotency-Key";

    /** Header carrying the client-supplied key (header strategy). Mutually exclusive with {@link #fieldPath()}. */
    String header() default "";

    /** JSONPath into the request body (body-field strategy). Mutually exclusive with {@link #header()}. */
    String fieldPath() default "";

    /** Store bean qualifier (e.g. {@code "redis"}, {@code "postgres"}). {@code ""} inherits the global default. */
    String store() default "";

    /** Response TTL as a {@code Duration} string (e.g. {@code "24h"}). {@code ""} inherits the global default. */
    String ttl() default "";

    /** Whether the client must supply the key (400 if absent). Fixed default; does not inherit the global. */
    boolean keyRequired() default true;

    /** Posture when the store is unavailable. {@code DEFAULT} inherits the global. */
    OnStoreFailure onStoreFailure() default OnStoreFailure.DEFAULT;

    /** Behavior for a concurrent request found in-progress. {@code DEFAULT} inherits the global. */
    WhenInProgress whenInProgress() default WhenInProgress.DEFAULT;

    /** Store-failure posture (see ADR 0001 / {@code CONTEXT.md}). */
    enum OnStoreFailure {
        /** Let the request through unprotected. */
        OPEN,
        /** Reject with 503. */
        CLOSED,
        /** Inherit the global default. */
        DEFAULT
    }

    /** Concurrency behavior for an in-progress key (see ADR 0002). */
    enum WhenInProgress {
        /** Immediate 409. */
        REJECT,
        /** Block until the primary completes, then replay. */
        WAIT,
        /** Inherit the global default. */
        DEFAULT
    }
}
