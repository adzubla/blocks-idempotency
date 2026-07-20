package io.adzubla.blocks.idempotency.store.redis;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import io.adzubla.blocks.idempotency.store.StoreUnavailableException;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves a genuine Redis outage (a stopped container, not a mock) surfaces as
 * {@link StoreUnavailableException} (Slice 015) - the signal {@link
 * IdempotencyEngine#before} catches to apply the
 * resolved {@code onStoreFailure} posture. The full fail-open/fail-closed
 * *reaction* to that exception is already unit-tested generically in {@code
 * idempotency-core} (Slice 007); {@link RedisIdempotencyOutageEndToEndTest}
 * additionally proves it end-to-end through real HTTP requests against this
 * store specifically.
 *
 * <p>A dedicated container (stopped mid-test, never restarted) rather than the
 * one shared by other test classes in this module - stopping it is
 * deliberately a one-way trip for this class.
 */
@Testcontainers
class RedisIdempotencyStoreOutageTest {

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    @Test
    void aStoppedRedisContainerSurfacesAsStoreUnavailable() {
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getRedisHost(), REDIS.getRedisPort()),
                LettuceClientConfiguration.builder().commandTimeout(Duration.ofSeconds(2)).build());
        connectionFactory.afterPropertiesSet();
        StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, new ObjectMapper(), "idempotency-outage-test:");
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "outage-1");

        // Sanity check: the store genuinely works against this container
        // before it's stopped - a false positive here (e.g. a
        // misconfigured connection that never worked) would make the
        // exception below meaningless.
        assertThat(store.reserve(key, "fp", Duration.ofSeconds(30)).outcome())
                .isEqualTo(ReservationResult.Outcome.RESERVED);

        REDIS.stop();

        assertThatThrownBy(() -> store.reserve(new EffectiveKey("/orders", "POST", "", "outage-2"), "fp", Duration.ofSeconds(30)))
                .isInstanceOf(StoreUnavailableException.class);
        connectionFactory.destroy();
    }
}
