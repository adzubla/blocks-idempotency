package io.adzubla.blocks.idempotency.model;

/**
 * A stored idempotency record. {@code response} is {@code null} while
 * {@link RecordState#IN_PROGRESS}, and may still be {@code null} once
 * {@link RecordState#COMPLETED} when the response is not replayable.
 */
public record IdempotencyRecord(RecordState state, String fingerprint, CachedResponse response) {

    public boolean isCompleted() {
        return state == RecordState.COMPLETED;
    }
}
