package io.adzubla.blocks.idempotency.store;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Sleeps the base poll interval plus a random jitter, capped so a caller
 * polling toward a deadline never oversleeps past what's left of its budget
 * (ADR 0002: "~100ms, with jitter"). Factored out of {@link IdempotencyStore}'s
 * default {@code await()} so the jitter math is unit-testable on its own.
 *
 * <p>An interrupt ends the sleep early: {@link #sleep} returns {@code false}
 * and leaves the thread's interrupt status set, so the caller can stop waiting
 * and its own caller still sees the interrupt.
 */
final class PollJitter {

    private PollJitter() {
    }

    /** @return {@code false} if interrupted (the interrupt status is re-set), {@code true} otherwise */
    static boolean sleep(Duration pollInterval, Duration pollJitter, Duration cap) {
        long jitterMillis = pollJitter.toMillis() > 0 ? ThreadLocalRandom.current().nextLong(pollJitter.toMillis() + 1) : 0;
        long delayMillis = Math.min(pollInterval.toMillis() + jitterMillis, cap.toMillis());
        try {
            Thread.sleep(delayMillis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
