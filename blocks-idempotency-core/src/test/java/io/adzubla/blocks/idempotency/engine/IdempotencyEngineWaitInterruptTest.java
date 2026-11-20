package io.adzubla.blocks.idempotency.engine;

import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * docs/issues/030-wait-mode-interrupt-uncontrolled-500.md: an interrupt while a
 * duplicate is parked in WAIT mode used to escape {@code before()} as a bare
 * {@link IllegalStateException} - a raw 500. It now ends the wait early, like
 * an elapsed {@code waitTimeout}: the duplicate is rejected with {@code
 * wait_timeout} (a controlled, retryable 409), and the thread's interrupt
 * status is left set for the caller to act on.
 *
 * <p>Deterministic: pre-setting the interrupt makes the first {@code
 * Thread.sleep} inside WAIT-mode polling throw immediately.
 */
class IdempotencyEngineWaitInterruptTest {

    @Test
    void anInterruptDuringWaitEndsTheWaitAsAControlledTimeoutRejection() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "key-1");
        Duration lockTtl = Duration.ofSeconds(30);
        // A primary holds the key IN_PROGRESS, so a same-fingerprint duplicate
        // takes the WAIT path rather than proceeding or colliding.
        store.reserve(key, "fp", lockTtl);
        IdempotencyEngine engine = new IdempotencyEngine(store, Duration.ofSeconds(5), Duration.ZERO);

        Thread.currentThread().interrupt();
        try {
            long start = System.nanoTime();
            EngineDecision decision = engine.before(key, "fp", lockTtl, OnStoreFailure.OPEN, WhenInProgress.WAIT, Duration.ofSeconds(30));
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            assertThat(decision).isEqualTo(new EngineDecision.Reject(RejectReason.TIMEOUT, lockTtl));
            assertThat(elapsedMillis).isLessThan(1_000); // returned on the interrupt, not after waitTimeout
            // The interrupt must remain observable to the caller, not be swallowed.
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Clear the flag so it can't leak into other tests on this thread.
            Thread.interrupted();
        }
    }
}
