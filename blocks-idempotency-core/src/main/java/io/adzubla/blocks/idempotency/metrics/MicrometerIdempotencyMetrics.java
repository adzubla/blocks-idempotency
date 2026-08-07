package io.adzubla.blocks.idempotency.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Records each outcome as one Micrometer counter, {@value #METRIC_NAME},
 * dimensioned by an {@value #TAG_OUTCOME} tag rather than four separate
 * counter names - the idiomatic Micrometer shape for an enumerable outcome
 * (sum/group by outcome in a dashboard).
 */
public final class MicrometerIdempotencyMetrics implements IdempotencyMetrics {

    static final String METRIC_NAME = "idempotency.outcomes";
    static final String TAG_OUTCOME = "outcome";

    private final Counter replay;
    private final Counter collision;
    private final Counter concurrency;
    private final Counter failOpen;
    private final Counter failClosed;
    private final Counter responseUnavailable;

    public MicrometerIdempotencyMetrics(MeterRegistry registry) {
        this.replay = counter(registry, "replay");
        this.collision = counter(registry, "collision");
        this.concurrency = counter(registry, "concurrency");
        this.failOpen = counter(registry, "fail_open");
        this.failClosed = counter(registry, "fail_closed");
        this.responseUnavailable = counter(registry, "response_unavailable");
    }

    private static Counter counter(MeterRegistry registry, String outcome) {
        return Counter.builder(METRIC_NAME).tag(TAG_OUTCOME, outcome).register(registry);
    }

    @Override
    public void recordReplay() {
        replay.increment();
    }

    @Override
    public void recordCollision() {
        collision.increment();
    }

    @Override
    public void recordConcurrency() {
        concurrency.increment();
    }

    @Override
    public void recordFailOpen() {
        failOpen.increment();
    }

    @Override
    public void recordFailClosed() {
        failClosed.increment();
    }

    @Override
    public void recordResponseUnavailable() {
        responseUnavailable.increment();
    }
}
