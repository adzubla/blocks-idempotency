package io.adzubla.blocks.idempotency.metrics;

import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;

/**
 * Discards every recording. The {@link IdempotencyEngine}'s
 * default when no {@link IdempotencyMetrics} is supplied, and what {@code
 * idempotency.metrics.enabled=false} wires in.
 */
public final class NoOpIdempotencyMetrics implements IdempotencyMetrics {

    public static final NoOpIdempotencyMetrics INSTANCE = new NoOpIdempotencyMetrics();

    private NoOpIdempotencyMetrics() {
    }

    @Override
    public void recordReplay() {
    }

    @Override
    public void recordCollision() {
    }

    @Override
    public void recordConcurrency() {
    }

    @Override
    public void recordFailOpen() {
    }
}
