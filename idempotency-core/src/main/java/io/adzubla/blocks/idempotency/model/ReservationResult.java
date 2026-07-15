package io.adzubla.blocks.idempotency.model;

import io.adzubla.blocks.idempotency.store.IdempotencyStore;

import java.util.Optional;

/**
 * Outcome of an atomic reservation attempt. Either the caller acquired the
 * reservation ({@link Outcome#RESERVED}) and becomes the primary, or a record
 * already exists ({@link Outcome#EXISTS}) and the caller must decide replay / 409 /
 * 422 from it.
 *
 * <p>{@code fenceToken} is only present on {@link Outcome#RESERVED}: an
 * opaque value identifying *this* reservation attempt, which the primary must
 * echo back to {@link IdempotencyStore#complete} /
 * {@link IdempotencyStore#release}. It fences off a
 * stale primary - one whose {@code lockTtl} already expired and was
 * superseded by a fresh reservation - from completing or releasing a record
 * that is no longer its own (see {@code CONTEXT.md} — Expiration).
 */
public record ReservationResult(Outcome outcome, Optional<IdempotencyRecord> existing, Optional<String> fenceToken) {

    public enum Outcome { RESERVED, EXISTS }

    public static ReservationResult reserved(String fenceToken) {
        return new ReservationResult(Outcome.RESERVED, Optional.empty(), Optional.of(fenceToken));
    }

    public static ReservationResult exists(IdempotencyRecord record) {
        return new ReservationResult(Outcome.EXISTS, Optional.of(record), Optional.empty());
    }
}
