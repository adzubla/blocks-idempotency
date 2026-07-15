package io.adzubla.blocks.idempotency.model;

/**
 * The single scoped key the library uses per request: endpoint + authenticated
 * principal + key value. Endpoints are isolated; {@code principal} falls back to a
 * sentinel on unauthenticated routes.
 *
 * <p>Each store maps this to its own representation (Postgres composite PK, Redis
 * hashed key).
 */
public record EffectiveKey(String method, String path, String principal, String value) {

    /** Sentinel used for the principal on routes without authentication. */
    public static final String NO_PRINCIPAL = "";
}
