package io.adzubla.blocks.idempotency.engine;

import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exposes a bug (docs/issues/030-wait-mode-interrupt-uncontrolled-500.md):
 * {@code PollJitter.sleep} re-sets the interrupt flag and throws
 * {@link IllegalStateException} on {@code InterruptedException}
 * ({@code store/PollJitter.java:22-25}). {@code IdempotencyEngine.waitForCompletion}
 * catches only {@code StoreUnavailableException} ({@code IdempotencyEngine.java:101-105}),
 * so a thread interrupt while a duplicate is parked in WAIT mode propagates out
 * of {@code before()} as an uncontrolled {@link IllegalStateException} - a raw
 * 500 - instead of a controlled idempotency outcome.
 *
 * <p>Deterministic: pre-setting the interrupt makes the first
 * {@code Thread.sleep} inside WAIT-mode polling throw immediately.
 *
 * <p>{@code @Disabled} so CI stays green while the fix is deferred; remove it to
 * reproduce the failure.
 */
class IdempotencyEngineWaitInterruptTest {

    @Test
    @Disabled("Exposes bug: docs/issues/030-wait-mode-interrupt-uncontrolled-500.md; remove @Disabled to reproduce")
    void interruptDuringWaitDoesNotLeakAnUncontrolledIllegalStateException() {
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-1");
        Duration lockTtl = Duration.ofSeconds(30);
        // A primary holds the key IN_PROGRESS, so a same-fingerprint duplicate
        // takes the WAIT path rather than proceeding or colliding.
        store.reserve(key, "fp", lockTtl);
        IdempotencyEngine engine = new IdempotencyEngine(store, Duration.ofMillis(100), Duration.ZERO);

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> engine.before(key, "fp", lockTtl,
                    OnStoreFailure.OPEN, WhenInProgress.WAIT, Duration.ofSeconds(5)))
                    .isNotInstanceOf(IllegalStateException.class);
            // The interrupt must remain observable to the caller, not be swallowed.
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Clear the flag so it can't leak into other tests on this thread.
            Thread.interrupted();
        }
    }
}
