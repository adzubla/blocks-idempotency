package io.adzubla.blocks.idempotency.store;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.IdempotencyRecord;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exposes a bug (docs/issues/031-default-await-clock-and-initial-check.md) in
 * {@link IdempotencyStore}'s default {@code await} ({@code IdempotencyStore.java:57-72}):
 * it sleeps a full poll interval <em>before</em> its first {@code find()}, with
 * no fast-path check. A duplicate that arrives just after the primary completed
 * pays an avoidable poll interval of latency even though the answer is already
 * available.
 *
 * <p>(The same default also computes its deadline from wall-clock
 * {@code Instant.now()} rather than the store's injectable {@code Clock} that
 * drives {@code InMemoryIdempotencyStore} expiry - see the issue for that
 * related half of the finding.)
 *
 * <p>Deterministic: an inline store whose {@code find()} already reports
 * {@code COMPLETED}; a correct {@code await} returns via an initial check
 * without sleeping the (large) poll interval.
 *
 * <p>{@code @Disabled} so CI stays green while the fix is deferred; remove it to
 * reproduce the failure.
 */
class IdempotencyStoreDefaultAwaitTest {

    /** Uses the interface default {@code await}; {@code find} is already terminal. */
    private static final IdempotencyStore ALREADY_COMPLETED = new IdempotencyStore() {
        @Override
        public ReservationResult reserve(EffectiveKey key, String fingerprint, Duration lockTtl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<IdempotencyRecord> find(EffectiveKey key) {
            return Optional.of(new IdempotencyRecord(RecordState.COMPLETED, "fp",
                    new CachedResponse(200, java.util.Map.of(), new byte[0])));
        }

        @Override
        public void complete(EffectiveKey key, String fenceToken, CachedResponse response, Duration responseTtl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void release(EffectiveKey key, String fenceToken) {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    @Disabled("Exposes bug: docs/issues/031-default-await-clock-and-initial-check.md; remove @Disabled to reproduce")
    void awaitReturnsPromptlyForAnAlreadyCompletedKeyInsteadOfSleepingAPollInterval() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-1");

        long start = System.nanoTime();
        Optional<IdempotencyRecord> result =
                ALREADY_COMPLETED.await(key, Duration.ofSeconds(10), Duration.ofSeconds(2), Duration.ZERO);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertThat(result).get().extracting(IdempotencyRecord::isCompleted).isEqualTo(true);
        assertThat(elapsedMillis).isLessThan(500); // FAILS today: sleeps the full 2s poll interval first
    }
}
