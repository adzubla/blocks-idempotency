package io.adzubla.blocks.idempotency.model;

/** Observable states of an effective key (see {@code CONTEXT.md} — Key lifecycle). */
public enum RecordState {
    /** Reserved atomically; the primary is still processing the effect. */
    IN_PROGRESS,
    /** Response written; repeats replay the cache. */
    COMPLETED
}
