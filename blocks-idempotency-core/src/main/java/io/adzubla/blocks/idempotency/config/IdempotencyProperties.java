package io.adzubla.blocks.idempotency.config;

import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

import java.time.Duration;

/**
 * Global defaults for idempotency, inherited by {@code @Idempotent} attributes left
 * at their sentinel. There is intentionally no {@code key-required} global default —
 * that is a fixed per-endpoint contract.
 */
@ConfigurationProperties(prefix = "idempotency")
public class IdempotencyProperties {

    /** Default store qualifier when the annotation leaves {@code store=""}. No default - must be set explicitly. */
    private String defaultStore = "";

    /** Default response TTL when the annotation leaves {@code ttl=""}. */
    private Duration defaultTtl = Duration.ofHours(24);

    /** Default store-failure posture. Must be {@code OPEN} or {@code CLOSED}, never {@code DEFAULT}. */
    private OnStoreFailure defaultOnStoreFailure = OnStoreFailure.OPEN;

    /** Default concurrency behavior. Must be {@code REJECT} or {@code WAIT}, never {@code DEFAULT}. */
    private WhenInProgress defaultWhenInProgress = WhenInProgress.REJECT;

    /** TTL of the in-progress reservation (anti-poisoning lock). */
    private Duration lockTtl = Duration.ofSeconds(30);

    /** How long a WAIT waiter blocks before giving up. */
    private Duration waitTimeout = Duration.ofSeconds(5);

    private final Key key = new Key();
    private final Scope scope = new Scope();
    private final Replay replay = new Replay();
    private final Metrics metrics = new Metrics();

    /** Responses larger than this are not cached (replay returns 409 response_unavailable). */
    private DataSize maxBodySize = DataSize.ofMegabytes(1);

    public static class Key {
        /** Raw key values longer than this are rejected (see {@link io.adzubla.blocks.idempotency.key.KeyFormat}). */
        private int maxLength = 255;

        public int getMaxLength() { return maxLength; }
        public void setMaxLength(int maxLength) { this.maxLength = maxLength; }
    }

    public static class Scope {
        private boolean principalEnabled = true;

        /**
         * Passed through verbatim to the active {@link
         * io.adzubla.blocks.idempotency.key.PrincipalClaimResolver} bean; otherwise
         * inert. The default resolver ({@link
         * io.adzubla.blocks.idempotency.key.DefaultPrincipalClaimResolver}) ignores
         * it and always scopes by {@code Principal#getName()} - register a custom
         * {@code PrincipalClaimResolver} bean to act on it (e.g. to pull a JWT claim).
         */
        private String principalClaim = "sub";

        public boolean isPrincipalEnabled() { return principalEnabled; }
        public void setPrincipalEnabled(boolean principalEnabled) { this.principalEnabled = principalEnabled; }
        public String getPrincipalClaim() { return principalClaim; }
        public void setPrincipalClaim(String principalClaim) { this.principalClaim = principalClaim; }
    }

    public static class Replay {
        private String headerName = "Idempotency-Replayed";
        /** Volatile headers not reproduced on replay. {@code Set-Cookie} is always blocked regardless. */
        private String[] headerDenylist = {"Date", "Set-Cookie", "traceparent", "tracestate"};

        public String getHeaderName() { return headerName; }
        public void setHeaderName(String headerName) { this.headerName = headerName; }
        public String[] getHeaderDenylist() { return headerDenylist; }
        public void setHeaderDenylist(String[] headerDenylist) { this.headerDenylist = headerDenylist; }
    }

    public static class Metrics {
        /** Whether outcome counters (replay/collision/concurrency/fail-open) are recorded. */
        private boolean enabled = true;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    public String getDefaultStore() { return defaultStore; }
    public void setDefaultStore(String defaultStore) { this.defaultStore = defaultStore; }
    public Duration getDefaultTtl() { return defaultTtl; }
    public void setDefaultTtl(Duration defaultTtl) { this.defaultTtl = defaultTtl; }
    public OnStoreFailure getDefaultOnStoreFailure() { return defaultOnStoreFailure; }
    public void setDefaultOnStoreFailure(OnStoreFailure defaultOnStoreFailure) { this.defaultOnStoreFailure = defaultOnStoreFailure; }
    public WhenInProgress getDefaultWhenInProgress() { return defaultWhenInProgress; }
    public void setDefaultWhenInProgress(WhenInProgress defaultWhenInProgress) { this.defaultWhenInProgress = defaultWhenInProgress; }
    public Duration getLockTtl() { return lockTtl; }
    public void setLockTtl(Duration lockTtl) { this.lockTtl = lockTtl; }
    public Duration getWaitTimeout() { return waitTimeout; }
    public void setWaitTimeout(Duration waitTimeout) { this.waitTimeout = waitTimeout; }
    public DataSize getMaxBodySize() { return maxBodySize; }
    public void setMaxBodySize(DataSize maxBodySize) { this.maxBodySize = maxBodySize; }
    public Key getKey() { return key; }
    public Scope getScope() { return scope; }
    public Replay getReplay() { return replay; }
    public Metrics getMetrics() { return metrics; }
}
