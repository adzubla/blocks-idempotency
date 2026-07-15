package io.adzubla.blocks.idempotency.store.redis;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@code reserve()} is genuinely atomic under real concurrency (Slice
 * 015's "atomic via Lua scripts") - a fresh key hit by many simultaneous
 * callers must yield exactly one {@code RESERVED} winner, never zero, never
 * more than one. This is Redis's own guarantee (a Lua script runs to
 * completion with no other command interleaved), not something the generic
 * {@code IdempotencyEngine}/contract-suite machinery would otherwise exercise
 * (the shared suite is single-threaded).
 */
@Testcontainers
class RedisIdempotencyStoreConcurrencyTest {

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final int CONCURRENT_CALLERS = 20;

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
    void exactlyOneOfManyConcurrentReservesOnTheSameKeyWins() throws Exception {
        StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, new ObjectMapper(), "idempotency-concurrency-test:");
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "race-1");

        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_CALLERS);
        try {
            List<Callable<ReservationResult>> callers = IntStream.range(0, CONCURRENT_CALLERS)
                    .<Callable<ReservationResult>>mapToObj(i -> () -> store.reserve(key, "fp", TTL))
                    .collect(Collectors.toList());

            List<Future<ReservationResult>> futures = executor.invokeAll(callers);
            long won = 0;
            long existed = 0;
            for (Future<ReservationResult> future : futures) {
                ReservationResult result = future.get(5, TimeUnit.SECONDS);
                if (result.outcome() == ReservationResult.Outcome.RESERVED) {
                    won++;
                } else {
                    existed++;
                }
            }

            assertThat(won).isEqualTo(1);
            assertThat(existed).isEqualTo(CONCURRENT_CALLERS - 1);
        } finally {
            executor.shutdownNow();
        }
    }
}
