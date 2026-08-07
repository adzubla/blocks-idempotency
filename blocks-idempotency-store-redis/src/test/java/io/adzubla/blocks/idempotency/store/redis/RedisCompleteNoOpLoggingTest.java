package io.adzubla.blocks.idempotency.store.redis;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the operator-visibility half of Slice 032
 * (docs/issues/032-redis-slow-primary-loses-response.md): when {@code complete()}
 * silently no-ops because the reservation is gone or superseded (its {@code
 * lock-ttl} elapsed before the handler finished), that gap must at least be
 * logged rather than being invisible.
 */
@Testcontainers
class RedisCompleteNoOpLoggingTest {

    private static final String PREFIX = "idempotency-nooplog-test:";

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    private LettuceConnectionFactory connectionFactory;
    private RedisIdempotencyStore store;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void setUp() {
        connectionFactory = new LettuceConnectionFactory(REDIS.getRedisHost(), REDIS.getRedisPort());
        connectionFactory.afterPropertiesSet();
        StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        store = new RedisIdempotencyStore(redisTemplate, new ObjectMapper(), PREFIX);

        appender = new ListAppender<>();
        appender.start();
        ((Logger) LoggerFactory.getLogger(RedisIdempotencyStore.class)).addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        ((Logger) LoggerFactory.getLogger(RedisIdempotencyStore.class)).detachAppender(appender);
        connectionFactory.destroy();
    }

    @Test
    void completingAVanishedReservationLogsAWarning() {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "slow-1");
        StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        String fenceToken = store.reserve(key, "fp", Duration.ofMillis(50)).fenceToken().orElseThrow();
        // Simulate the reservation's lock-ttl having elapsed before the handler finished.
        redisTemplate.delete(PREFIX + HexFormat.of().formatHex(key.digestBytes()));

        store.complete(key, fenceToken, new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes()), Duration.ofHours(24));

        assertThat(appender.list)
                .anySatisfy(event -> {
                    assertThat(event.getLevel()).isEqualTo(Level.WARN);
                    assertThat(event.getFormattedMessage()).contains("no-op").contains("/orders").contains("slow-1");
                });
    }

    @Test
    void completingALiveReservationLogsNothing() {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "fast-1");
        String fenceToken = store.reserve(key, "fp", Duration.ofSeconds(30)).fenceToken().orElseThrow();

        store.complete(key, fenceToken, new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes()), Duration.ofHours(24));

        assertThat(appender.list).isEmpty();
    }
}
