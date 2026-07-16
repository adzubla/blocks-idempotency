package io.adzubla.blocks.idempotency.store;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Sleeps the base poll interval plus a random jitter, capped so a caller
 * polling toward a deadline never oversleeps past what's left of its budget
 * (ADR 0002: "~100ms, with jitter"). Factored out of {@link IdempotencyStore}'s
 * default {@code await()} so the jitter math is unit-testable on its own.
 */
final class PollJitter {

    private PollJitter() {
    }

    static void sleep(Duration pollInterval, Duration pollJitter, Duration cap) {
        long jitterMillis = pollJitter.toMillis() > 0 ? ThreadLocalRandom.current().nextLong(pollJitter.toMillis() + 1) : 0;
        long delayMillis = Math.min(pollInterval.toMillis() + jitterMillis, cap.toMillis());
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the idempotency key to complete", e);
        }
    }
}
