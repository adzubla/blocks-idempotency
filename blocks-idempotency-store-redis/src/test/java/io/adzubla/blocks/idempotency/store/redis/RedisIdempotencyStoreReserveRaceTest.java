package io.adzubla.blocks.idempotency.store.redis;

import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import io.adzubla.blocks.idempotency.store.StoreUnavailableException;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers the reservation re-read race
 * (docs/issues/029-store-reserve-conflict-vanished-record-500.md): {@code
 * reserve()} checks for a conflict atomically (the Lua {@code EXISTS}), then
 * re-reads the existing record in a <em>separate</em> round-trip. If the
 * conflicting record vanishes in that window (its lock-ttl expired, or it was
 * released), the re-read finds nothing - the key is free now.
 *
 * <p>Before the fix that threw a bare {@code IllegalStateException} which {@code
 * guarded(...)} did not map, so it escaped as a raw 500 bypassing the {@code
 * onStoreFailure} posture. Now {@code reserve()} retries (winning once the key
 * is free) and, only if it keeps racing, surfaces {@link
 * StoreUnavailableException} so the posture applies.
 *
 * <p>Pure Mockito, no Redis.
 */
class RedisIdempotencyStoreReserveRaceTest {

    private final EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-1");

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void reserveRetriesWhenTheConflictingRecordVanishedAndWinsOnTheRetry() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations hashOps = mock(HashOperations.class);

        // First attempt loses the conflict (0); the retry wins (1).
        when(redis.execute(any(RedisScript.class), anyList(), any(), any(), any())).thenReturn(0L, 1L);
        // The record that blocked the first attempt is already gone by the re-read.
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries(any())).thenReturn(Map.of());

        RedisIdempotencyStore store = new RedisIdempotencyStore(redis, new ObjectMapper(), "idem:");

        ReservationResult result = store.reserve(key, "fp", Duration.ofSeconds(30));

        assertThat(result.outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
        assertThat(result.fenceToken()).isPresent();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void reserveThatKeepsRacingSurfacesAsStoreUnavailableRatherThanARaw500() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations hashOps = mock(HashOperations.class);

        // Every attempt loses the conflict...
        when(redis.execute(any(RedisScript.class), anyList(), any(), any(), any())).thenReturn(0L);
        // ...and the record is gone on every re-read (a permanently flapping key).
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries(any())).thenReturn(Map.of());

        RedisIdempotencyStore store = new RedisIdempotencyStore(redis, new ObjectMapper(), "idem:");

        assertThatThrownBy(() -> store.reserve(key, "fp", Duration.ofSeconds(30)))
                .isInstanceOf(StoreUnavailableException.class);
    }
}
