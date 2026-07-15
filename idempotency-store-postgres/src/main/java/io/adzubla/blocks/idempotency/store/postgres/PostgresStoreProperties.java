package io.adzubla.blocks.idempotency.store.postgres;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Postgres-store-specific settings (Slice 018) - distinct from {@code
 * idempotency.lock-ttl} (the reservation's own staleness TTL, {@code
 * IdempotencyProperties}, store-agnostic): these instead bound the native
 * concurrency mechanism that's unique to this store (ADR 0001/0002/0003).
 */
@ConfigurationProperties(prefix = "idempotency.postgres")
public class PostgresStoreProperties {

    /**
     * Upper bound on how long a concurrent {@code reserve()} blocks on the
     * reservation row's lock (ADR 0001/0002's "bounded by {@code
     * lock_timeout}"). Once exceeded, the waiter gives up rather than holding
     * its thread indefinitely and is treated as finding the key still
     * in-progress (fenced through as {@code RejectReason.IN_PROGRESS}).
     */
    private Duration lockTimeout = Duration.ofSeconds(2);

    private final Cleanup cleanup = new Cleanup();

    public Duration getLockTimeout() { return lockTimeout; }
    public void setLockTimeout(Duration lockTimeout) { this.lockTimeout = lockTimeout; }
    public Cleanup getCleanup() { return cleanup; }

    /** The scheduled job that deletes rows past {@code expires_at} - Postgres has no native TTL. */
    public static class Cleanup {
        private boolean enabled = true;
        private Duration interval = Duration.ofMinutes(5);

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public Duration getInterval() { return interval; }
        public void setInterval(Duration interval) { this.interval = interval; }
    }
}
