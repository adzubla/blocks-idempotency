package io.adzubla.blocks.idempotency.store;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.IdempotencyRecord;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link IdempotencyStore}'s default {@code await}
 * (docs/issues/031-default-await-clock-and-initial-check.md,
 * docs/issues/030-wait-mode-interrupt-uncontrolled-500.md): it checks {@code
 * find()} once before its first sleep, so a duplicate that arrives just after
 * the primary completed doesn't pay a poll interval of latency; and an
 * interrupt ends the wait early with the last record observed, leaving the
 * interrupt status set.
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
    void awaitReturnsPromptlyForAnAlreadyCompletedKeyInsteadOfSleepingAPollInterval() {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "key-1");

        long start = System.nanoTime();
        Optional<IdempotencyRecord> result =
                ALREADY_COMPLETED.await(key, Duration.ofSeconds(10), Duration.ofSeconds(2), Duration.ZERO);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertThat(result).get().extracting(IdempotencyRecord::isCompleted).isEqualTo(true);
        assertThat(elapsedMillis).isLessThan(500); // not the 2s poll interval
    }

    @Test
    void anInterruptEndsTheWaitWithTheLastObservedRecordAndKeepsTheInterruptStatus() {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "key-1");
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        store.reserve(key, "fp", Duration.ofSeconds(30));

        Thread.currentThread().interrupt();
        try {
            long start = System.nanoTime();
            Optional<IdempotencyRecord> result = store.await(key, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ZERO);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            assertThat(result).get().extracting(IdempotencyRecord::state).isEqualTo(RecordState.IN_PROGRESS);
            assertThat(elapsedMillis).isLessThan(1_000);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }
}
