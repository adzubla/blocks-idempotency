package io.adzubla.blocks.idempotency.store.redis;

import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.store.StoreUnavailableException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exposes a bug (docs/issues/029-store-reserve-conflict-vanished-record-500.md):
 * {@code reserve()} checks for a conflict atomically (the Lua {@code EXISTS}),
 * but then re-reads the existing record in a <em>separate</em> round-trip
 * ({@code RedisIdempotencyStore.java:129-130}). If the conflicting record
 * vanishes in that window (its lock-ttl expired, or it was released), the
 * re-read returns empty and the code throws a bare {@link IllegalStateException}
 * - which {@code guarded(...)} does not map, so it escapes as a raw 500 and
 * bypasses the configured {@code onStoreFailure} posture that exists precisely
 * to keep store hiccups from becoming uncontrolled 500s.
 *
 * <p>Pure Mockito, no Redis: the reserve script is stubbed to report "lost the
 * conflict" ({@code 0}) and the follow-up hash read to report "gone" (empty).
 *
 * <p>{@code @Disabled} so CI stays green while the fix is deferred; remove it to
 * reproduce the failure.
 */
class RedisIdempotencyStoreReserveRaceTest {

    private final EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-1");

    @Test
    @Disabled("Exposes bug: docs/issues/029-store-reserve-conflict-vanished-record-500.md; remove @Disabled to reproduce")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void reserveMapsAVanishedConflictingRecordToStoreUnavailableRatherThanARaw500() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations hashOps = mock(HashOperations.class);

        // Lost the reservation conflict: the record existed a moment ago.
        when(redis.execute(any(RedisScript.class), anyList(), any(), any(), any())).thenReturn(0L);
        // ...but by the re-read it is gone (lock-ttl expired / released).
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries(any())).thenReturn(Map.of());

        RedisIdempotencyStore store = new RedisIdempotencyStore(redis, new ObjectMapper(), "idem:");

        assertThatThrownBy(() -> store.reserve(key, "fp", Duration.ofSeconds(30)))
                .isInstanceOf(StoreUnavailableException.class);
    }
}
