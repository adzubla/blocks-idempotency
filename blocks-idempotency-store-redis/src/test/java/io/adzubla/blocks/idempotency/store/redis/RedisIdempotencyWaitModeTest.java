package io.adzubla.blocks.idempotency.store.redis;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.engine.EngineDecision;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the core's generic WAIT-mode polling ({@link
 * IdempotencyEngine} polls only via {@link IdempotencyStore#find},
 * ADR 0002's default model) correctly observes a real Redis-backed primary's
 * completion (Slice 015) - not assumed to inherit correctness from the
 * generic engine, exercised against Testcontainers Redis rather than the
 * in-memory store the rest of the engine's own test suite uses.
 */
@Testcontainers
class RedisIdempotencyWaitModeTest {

    private static final Duration LOCK_TTL = Duration.ofSeconds(30);
    private static final Duration RESPONSE_TTL = Duration.ofSeconds(30);
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration PRIMARY_COMPLETION_DELAY = Duration.ofMillis(300);

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    private static LettuceConnectionFactory connectionFactory;

    @BeforeAll
    static void connect() {
        connectionFactory = new LettuceConnectionFactory(REDIS.getRedisHost(), REDIS.getRedisPort());
        connectionFactory.afterPropertiesSet();
    }

    @AfterAll
    static void disconnect() {
        connectionFactory.destroy();
    }

    @Test
    void aWaitModeCallerObservesARealRedisBackedPrimarysCompletion() throws Exception {
        StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, new ObjectMapper(), "idempotency-wait-test:");
        IdempotencyEngine engine = new IdempotencyEngine(store);
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "wait-1");
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());

        EngineDecision primaryDecision = engine.before(key, "fp", LOCK_TTL, OnStoreFailure.OPEN, WhenInProgress.REJECT, WAIT_TIMEOUT);
        assertThat(primaryDecision).isInstanceOf(EngineDecision.Proceed.class);
        String fenceToken = ((EngineDecision.Proceed) primaryDecision).fenceToken();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> primaryCompletion = executor.submit(() -> {
                try {
                    Thread.sleep(PRIMARY_COMPLETION_DELAY.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                engine.complete(key, fenceToken, response, RESPONSE_TTL);
            });

            long start = System.nanoTime();
            EngineDecision waiterDecision = engine.before(key, "fp", LOCK_TTL, OnStoreFailure.OPEN, WhenInProgress.WAIT, WAIT_TIMEOUT);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

            primaryCompletion.get(5, TimeUnit.SECONDS);

            assertThat(waiterDecision).isInstanceOf(EngineDecision.Replay.class);
            assertThat(((EngineDecision.Replay) waiterDecision).response()).isEqualTo(response);
            // Genuinely polled past the primary's completion delay, not an
            // instant/coincidental result.
            assertThat(elapsed).isGreaterThanOrEqualTo(PRIMARY_COMPLETION_DELAY.minusMillis(50));
        } finally {
            executor.shutdown();
        }
    }
}
