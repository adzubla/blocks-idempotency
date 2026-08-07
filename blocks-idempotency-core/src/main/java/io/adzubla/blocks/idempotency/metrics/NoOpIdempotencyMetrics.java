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
    public void recordReplay(String route, String handler) {
    }

    @Override
    public void recordCollision(String route, String handler) {
    }

    @Override
    public void recordConcurrency(String route, String handler) {
    }

    @Override
    public void recordFailOpen(String route, String handler) {
    }

    @Override
    public void recordFailClosed(String route, String handler) {
    }

    @Override
    public void recordResponseUnavailable(String route, String handler) {
    }
}
