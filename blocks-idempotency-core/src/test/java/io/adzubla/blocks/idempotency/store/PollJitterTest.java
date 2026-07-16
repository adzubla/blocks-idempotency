package io.adzubla.blocks.idempotency.store;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class PollJitterTest {

    @Test
    void delayVariesWithinTheConfiguredJitterRangeAndNeverUndershootsPollInterval() {
        Duration pollInterval = Duration.ofMillis(10);
        Duration pollJitter = Duration.ofMillis(20);
        Duration cap = pollInterval.plus(pollJitter).plusMillis(1);

        Set<Long> observedDelaysMillis = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            long start = System.nanoTime();
            PollJitter.sleep(pollInterval, pollJitter, cap);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            observedDelaysMillis.add(elapsedMillis);
            assertThat(elapsedMillis).isGreaterThanOrEqualTo(pollInterval.toMillis());
        }

        // Real jitter means 20 samples shouldn't all collapse to one delay.
        assertThat(observedDelaysMillis).hasSizeGreaterThan(1);
    }

    @Test
    void neverSleepsPastTheGivenCap() {
        Duration pollInterval = Duration.ofMillis(200);
        Duration pollJitter = Duration.ofMillis(200);
        Duration cap = Duration.ofMillis(30);

        long start = System.nanoTime();
        PollJitter.sleep(pollInterval, pollJitter, cap);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMillis).isLessThan(cap.toMillis() + 20);
    }
}
