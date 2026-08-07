package io.adzubla.blocks.idempotency.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Records each outcome as one Micrometer counter, {@value #METRIC_NAME},
 * dimensioned by an {@value #TAG_OUTCOME} tag rather than six separate
 * counter names - the idiomatic Micrometer shape for an enumerable outcome
 * (sum/group by outcome in a dashboard) - plus {@value #TAG_ROUTE}/
 * {@value #TAG_HANDLER} so an outcome can be attributed to a specific
 * endpoint/listener, not just the aggregate. {@code route}/{@code handler}
 * are bounded by the number of distinct {@code @Idempotent} methods in the
 * application, not by request volume, so tagging on them doesn't blow up
 * cardinality the way tagging on the raw key value would.
 *
 * <p>Counters are looked up (not pre-built) on every call: {@code
 * Counter.builder(...).register(registry)} is a get-or-create against the
 * registry's own meter cache keyed by name+tags, so repeat calls with the
 * same route/handler/outcome return the same counter instance rather than
 * creating a new one.
 */
public final class MicrometerIdempotencyMetrics implements IdempotencyMetrics {

    static final String METRIC_NAME = "idempotency.outcomes";
    static final String TAG_OUTCOME = "outcome";
    static final String TAG_ROUTE = "route";
    static final String TAG_HANDLER = "handler";

    private final MeterRegistry registry;

    public MicrometerIdempotencyMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    private void increment(String outcome, String route, String handler) {
        Counter.builder(METRIC_NAME)
                .tag(TAG_OUTCOME, outcome)
                .tag(TAG_ROUTE, route)
                .tag(TAG_HANDLER, handler)
                .register(registry)
                .increment();
    }

    @Override
    public void recordReplay(String route, String handler) {
        increment("replay", route, handler);
    }

    @Override
    public void recordCollision(String route, String handler) {
        increment("collision", route, handler);
    }

    @Override
    public void recordConcurrency(String route, String handler) {
        increment("concurrency", route, handler);
    }

    @Override
    public void recordFailOpen(String route, String handler) {
        increment("fail_open", route, handler);
    }

    @Override
    public void recordFailClosed(String route, String handler) {
        increment("fail_closed", route, handler);
    }

    @Override
    public void recordResponseUnavailable(String route, String handler) {
        increment("response_unavailable", route, handler);
    }
}
