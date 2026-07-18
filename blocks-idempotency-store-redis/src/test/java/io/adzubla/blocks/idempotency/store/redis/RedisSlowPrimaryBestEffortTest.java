package io.adzubla.blocks.idempotency.store.redis;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
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
 * Documents a best-effort limitation
 * (docs/issues/032-redis-slow-primary-loses-response.md): Redis lifecycle rides
 * the key's own TTL, so when a legitimately-slow primary runs longer than its
 * {@code lock-ttl}, the record's Redis key is already gone by the time
 * {@code complete()} runs - the Lua {@code HGET token} returns nil, the script
 * no-ops, and the response is silently dropped (never cached). Postgres and the
 * in-memory store keep the reservation alive across a slow handler and cache it.
 *
 * <p>Deterministic: the {@code lock-ttl} expiry is simulated by deleting the
 * record key (TTL expiry in Redis <em>is</em> the key being removed), rather
 * than sleeping past a real TTL.
 *
 * <p>This is the documented "best-effort vs exactly-once" split, not a contract
 * bug - see the issue. {@code @Disabled}; enabling it fails, demonstrating the
 * dropped response.
 */
@Testcontainers
class RedisSlowPrimaryBestEffortTest {

    private static final String PREFIX = "idempotency-besteffort-test:";

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    @Test
    @Disabled("Documents limitation: docs/issues/032-redis-slow-primary-loses-response.md; remove @Disabled to reproduce")
    void aSlowPrimaryPastItsLockTtlStillHasItsResponseCached() {
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(REDIS.getRedisHost(), REDIS.getRedisPort());
        connectionFactory.afterPropertiesSet();
        StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, new ObjectMapper(), PREFIX);
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "slow-1");

        String fenceToken = store.reserve(key, "fp", Duration.ofMillis(50)).fenceToken().orElseThrow();
        // The handler outran lock-ttl: the record key is gone (TTL expiry == key removed).
        redisTemplate.delete(PREFIX + HexFormat.of().formatHex(key.digestBytes()));

        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());
        store.complete(key, fenceToken, response, Duration.ofHours(24));

        // The primary's effect ran and it completed; its response should be replayable.
        assertThat(store.find(key)).get()
                .satisfies(record -> assertThat(record.isCompleted()).isTrue());
        connectionFactory.destroy();
    }
}
