package io.adzubla.blocks.idempotency.engine;

import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.metrics.MicrometerIdempotencyMetrics;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves Slice 011's "counters increment for replay, collision (422),
 * concurrency (409), fail-open, fail-closed, and response-unavailable"
 * end-to-end through {@link IdempotencyEngine#before}, using a real {@link
 * SimpleMeterRegistry} rather than a fake recorder. Also proves each counter
 * is attributable to a specific route/handler, not just an application-wide
 * aggregate.
 */
class IdempotencyEngineMetricsTest {

    private static final String METRIC_NAME = "idempotency.outcomes";
    private static final String TAG_OUTCOME = "outcome";
    private static final String TAG_ROUTE = "route";
    private static final String TAG_HANDLER = "handler";

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
    private final IdempotencyEngine engine = new IdempotencyEngine(store, new MicrometerIdempotencyMetrics(registry));
    private final Duration lockTtl = Duration.ofSeconds(30);
    private final Duration responseTtl = Duration.ofHours(24);
    private final Duration waitTimeout = Duration.ofSeconds(5);
    private final OnStoreFailure openPosture = OnStoreFailure.OPEN;
    private final WhenInProgress rejectMode = WhenInProgress.REJECT;

    @Test
    void replayIncrementsTheReplayCounter() {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "key-1");
        EngineDecision proceed = engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);
        String fenceToken = ((EngineDecision.Proceed) proceed).fenceToken();
        engine.complete(key, fenceToken, new CachedResponse(201, Map.of(), "{}".getBytes()), responseTtl);

        engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(counterValue("replay")).isEqualTo(1.0);
    }

    @Test
    void collisionIncrementsTheCollisionCounter() {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "key-2");
        engine.before(key, "fp-original", lockTtl, openPosture, rejectMode, waitTimeout);

        engine.before(key, "fp-different", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(counterValue("collision")).isEqualTo(1.0);
    }

    @Test
    void rejectIncrementsTheConcurrencyCounter() {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "key-3");
        engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(counterValue("concurrency")).isEqualTo(1.0);
    }

    @Test
    void waitTimeoutAlsoIncrementsTheConcurrencyCounter() {
        // Proves the counter is centralized across all three Reject call
        // sites (immediate REJECT, WAIT-timeout, key-gone), not just the
        // immediate one covered above.
        IdempotencyEngine waitEngine = new IdempotencyEngine(
                store, Duration.ofMillis(15), Duration.ofMillis(5), new MicrometerIdempotencyMetrics(registry));
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "key-4");
        engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        waitEngine.before(key, "fp", lockTtl, openPosture, WhenInProgress.WAIT, Duration.ofMillis(80));

        assertThat(counterValue("concurrency")).isEqualTo(1.0);
    }

    @Test
    void failOpenIncrementsTheFailOpenCounter() {
        store.setUnavailable(true);
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "key-5");

        engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(counterValue("fail_open")).isEqualTo(1.0);
    }

    @Test
    void failClosedIncrementsTheFailClosedCounter() {
        store.setUnavailable(true);
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "key-8");

        engine.before(key, "fp", lockTtl, OnStoreFailure.CLOSED, rejectMode, waitTimeout);

        assertThat(counterValue("fail_closed")).isEqualTo(1.0);
    }

    @Test
    void completedButNotReplayableIncrementsTheResponseUnavailableCounter() {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "key-9");
        EngineDecision proceed = engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);
        String fenceToken = ((EngineDecision.Proceed) proceed).fenceToken();
        engine.complete(key, fenceToken, CachedResponse.empty(), responseTtl);

        engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(counterValue("response_unavailable")).isEqualTo(1.0);
    }

    @Test
    void countersAccumulateAcrossMultipleOccurrencesOfTheSameOutcome() {
        EffectiveKey key1 = new EffectiveKey("/orders", "POST", "", "key-6");
        EffectiveKey key2 = new EffectiveKey("/orders", "POST", "", "key-7");
        engine.before(key1, "fp", lockTtl, openPosture, rejectMode, waitTimeout);
        engine.before(key2, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        engine.before(key1, "fp", lockTtl, openPosture, rejectMode, waitTimeout);
        engine.before(key2, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(counterValue("concurrency")).isEqualTo(2.0);
    }

    @Test
    void collisionsOnDifferentRoutesAreAttributedSeparately() {
        EffectiveKey ordersKey = new EffectiveKey("/orders", "POST", "", "key-10");
        EffectiveKey refundsKey = new EffectiveKey("/refunds", "POST", "", "key-11");
        engine.before(ordersKey, "fp-original", lockTtl, openPosture, rejectMode, waitTimeout);
        engine.before(refundsKey, "fp-original", lockTtl, openPosture, rejectMode, waitTimeout);

        engine.before(ordersKey, "fp-different", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(counterValue("collision", "/orders", "POST")).isEqualTo(1.0);
        assertThat(registry.find(METRIC_NAME).tag(TAG_OUTCOME, "collision").tag(TAG_ROUTE, "/refunds").counter()).isNull();
    }

    private double counterValue(String outcome) {
        return registry.get(METRIC_NAME).tag(TAG_OUTCOME, outcome).counter().count();
    }

    private double counterValue(String outcome, String route, String handler) {
        return registry.get(METRIC_NAME).tag(TAG_OUTCOME, outcome).tag(TAG_ROUTE, route).tag(TAG_HANDLER, handler).counter().count();
    }
}
