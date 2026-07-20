package io.adzubla.blocks.idempotency.model;

import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;

import java.nio.charset.StandardCharsets;

/**
 * The single scoped key the library uses per request: operation identity +
 * authenticated principal + key value. {@code route} is where it came in on
 * (HTTP path, or a message destination/topic); {@code handler} is which code
 * processes it (HTTP method+controller, or a listener id/consumer group).
 * Operations are isolated; {@code principal} falls back to a sentinel on
 * unauthenticated/unscoped routes.
 *
 * <p>Each store maps this to its own representation (Postgres composite PK, Redis
 * hashed key).
 */
public record EffectiveKey(String route, String handler, String principal, String value) {

    /** Sentinel used for the principal on routes without authentication. */
    public static final String NO_PRINCIPAL = "";

    /**
     * SHA-256 over {@code route}, {@code handler}, {@code principal}, and
     * {@code value}, joined with a {@code NUL} separator - a fixed-length,
     * addressable form a store can key its records on instead of the raw
     * components.
     */
    public byte[] digestBytes() {
        return Fingerprint.digestBytes(
                route.getBytes(StandardCharsets.UTF_8),
                handler.getBytes(StandardCharsets.UTF_8),
                principal.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8));
    }
}
