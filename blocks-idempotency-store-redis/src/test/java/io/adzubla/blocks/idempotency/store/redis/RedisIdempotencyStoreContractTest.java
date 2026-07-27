package io.adzubla.blocks.idempotency.store.redis;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.IdempotencyStoreContractTest;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the shared {@link IdempotencyStoreContractTest} suite (Slice 014)
 * against a real Testcontainers Redis, proving {@link RedisIdempotencyStore}
 * honors the same {@code reserve}/{@code find}/{@code complete}/{@code
 * release} contract as every other store (Slice 015) - including TTL
 * expiration and fence-token no-ops, both of which Redis realizes natively
 * (key TTL, Lua-checked token) rather than the conditional-supersession SQL
 * Postgres needs.
 */
@Testcontainers
class RedisIdempotencyStoreContractTest extends IdempotencyStoreContractTest {

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

    @Override
    protected IdempotencyStore createStore() {
        StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        return new RedisIdempotencyStore(redisTemplate, new ObjectMapper(), "idempotency-test:");
    }

    /**
     * Redis-specific: a Hash field is a Redis string, but the response body
     * is arbitrary bytes, not necessarily UTF-8 (a gzip body, a binary
     * upload receipt, ...). Proves the store's Base64 encoding round-trips
     * genuinely non-UTF-8 bytes losslessly, not just the JSON text every
     * shared contract-suite test happens to use.
     */
    @Test
    void aNonUtf8ResponseBodyRoundTripsLosslessly() {
        IdempotencyStore store = createStore();
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "binary-1");
        byte[] binaryBody = {0x00, 0x01, (byte) 0xFF, (byte) 0x80, 0x7F, (byte) 0xC3, (byte) 0x28};
        String fenceToken = store.reserve(key, "fp", Duration.ofSeconds(30)).fenceToken().orElseThrow();

        store.complete(key, fenceToken, new CachedResponse(201, Map.of(), binaryBody), Duration.ofSeconds(30));

        assertThat(store.find(key).orElseThrow().response().body()).isEqualTo(binaryBody);
        ReservationResult repeat = store.reserve(key, "fp", Duration.ofSeconds(30));
        assertThat(repeat.existing().orElseThrow().response().body()).isEqualTo(binaryBody);
    }
}
