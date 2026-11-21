package io.adzubla.blocks.idempotency.store;

import io.adzubla.blocks.idempotency.model.EffectiveKey;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Expired entries are removed, so the store doesn't grow without bound. */
class InMemoryIdempotencyStoreEvictionTest {

    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final Clock clock = new Clock() {
        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    };

    @Test
    void expiredEntriesAreEvictedByALaterSweep() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore(clock);
        int expiring = InMemoryIdempotencyStore.SWEEP_EVERY_RESERVATIONS - 1;
        for (int i = 0; i < expiring; i++) {
            store.reserve(key("old-" + i), "fp", Duration.ofSeconds(1));
        }
        assertThat(store.size()).isEqualTo(expiring);

        now.set(now.get().plusSeconds(2));
        store.reserve(key("fresh"), "fp", Duration.ofSeconds(30));

        assertThat(store.size()).isEqualTo(1);
        assertThat(store.find(key("fresh"))).isPresent();
    }

    @Test
    void liveEntriesSurviveASweep() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore(clock);
        for (int i = 0; i < InMemoryIdempotencyStore.SWEEP_EVERY_RESERVATIONS; i++) {
            store.reserve(key("live-" + i), "fp", Duration.ofMinutes(5));
        }

        assertThat(store.size()).isEqualTo(InMemoryIdempotencyStore.SWEEP_EVERY_RESERVATIONS);
    }

    private static EffectiveKey key(String value) {
        return new EffectiveKey("/orders", "POST", "", value);
    }
}
