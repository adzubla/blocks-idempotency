package io.adzubla.blocks.idempotency.store;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.IdempotencyRecord;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.model.ReservationResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory {@link IdempotencyStore}, for tests and single-instance use (state
 * is per JVM, so it does not deduplicate across instances; register it with
 * {@code @Bean(InMemoryIdempotencyStore.QUALIFIER)}). Properties:
 * <ul>
 *   <li>an atomic {@code reserve} via {@code compute};</li>
 *   <li>TTL-aware: a record past its {@code lockTtl}/{@code responseTtl} is
 *       treated as absent, so a repeat re-executes exactly as a brand-new key
 *       would; the {@link Clock} is injectable so tests can advance time;</li>
 *   <li>fence-tokened: {@code complete}/{@code release} no-op if the caller's
 *       token no longer matches the current reservation, so a primary whose
 *       lock already expired and was superseded can't clobber the new
 *       primary's record;</li>
 *   <li>no native wait support: inherits {@link IdempotencyStore}'s default
 *       {@code await()}, which polls {@link #find};</li>
 *   <li>expired records are evicted: every {@value #SWEEP_EVERY_RESERVATIONS}th
 *       {@code reserve} removes all expired entries, so the map holds only
 *       records still inside their TTL (plus at most that many reservations'
 *       worth of expired ones) instead of growing without bound;</li>
 *   <li>{@link #setUnavailable} simulates an outage (every operation throws
 *       {@link StoreUnavailableException}) for exercising the {@code
 *       onStoreFailure} posture.</li>
 * </ul>
 */
public class InMemoryIdempotencyStore implements IdempotencyStore {

    public static final String QUALIFIER = "in-memory";

    /** How many reservations pass between two sweeps of expired entries. */
    static final int SWEEP_EVERY_RESERVATIONS = 1024;

    private final Map<EffectiveKey, TimestampedRecord> records = new ConcurrentHashMap<>();
    private final Clock clock;
    private volatile boolean unavailable = false;
    private final AtomicInteger reservationsSinceSweep = new AtomicInteger();

    public InMemoryIdempotencyStore() {
        this(Clock.systemUTC());
    }

    public InMemoryIdempotencyStore(Clock clock) {
        this.clock = clock;
    }

    /** Simulates the store becoming unreachable (or recovering, when {@code false}). */
    public void setUnavailable(boolean unavailable) {
        this.unavailable = unavailable;
    }

    @Override
    public ReservationResult reserve(EffectiveKey key, String fingerprint, Duration lockTtl) {
        requireAvailable();
        Instant now = clock.instant();
        if (reservationsSinceSweep.incrementAndGet() >= SWEEP_EVERY_RESERVATIONS) {
            reservationsSinceSweep.set(0);
            evictExpired(now);
        }
        AtomicReference<ReservationResult> outcome = new AtomicReference<>();
        records.compute(key, (k, existing) -> {
            if (existing == null || existing.isExpired(now)) {
                String fenceToken = UUID.randomUUID().toString();
                outcome.set(ReservationResult.reserved(fenceToken));
                return new TimestampedRecord(
                        new IdempotencyRecord(RecordState.IN_PROGRESS, fingerprint, null), now.plus(lockTtl), fenceToken);
            }
            outcome.set(ReservationResult.exists(existing.record()));
            return existing;
        });
        return outcome.get();
    }

    @Override
    public Optional<IdempotencyRecord> find(EffectiveKey key) {
        requireAvailable();
        TimestampedRecord entry = records.get(key);
        return entry == null || entry.isExpired(clock.instant()) ? Optional.empty() : Optional.of(entry.record());
    }

    @Override
    public void complete(EffectiveKey key, String fenceToken, CachedResponse response, Duration responseTtl) {
        requireAvailable();
        Instant expiresAt = clock.instant().plus(responseTtl);
        records.computeIfPresent(key, (k, entry) -> entry.fenceToken().equals(fenceToken)
                ? new TimestampedRecord(
                        new IdempotencyRecord(RecordState.COMPLETED, entry.record().fingerprint(), response), expiresAt, fenceToken)
                : entry);
    }

    @Override
    public void release(EffectiveKey key, String fenceToken) {
        requireAvailable();
        records.computeIfPresent(key, (k, entry) -> entry.fenceToken().equals(fenceToken) ? null : entry);
    }

    /**
     * Removes every entry expired at {@code now}. Safe against a concurrent
     * {@code reserve} of the same key: {@code ConcurrentHashMap}'s entry-set
     * {@code removeIf} removes an entry only if it still maps to the value that
     * was tested, so a record that was just replaced is left alone.
     */
    private void evictExpired(Instant now) {
        records.entrySet().removeIf(entry -> entry.getValue().isExpired(now));
    }

    /** Number of entries held, expired or not. For tests. */
    int size() {
        return records.size();
    }

    private void requireAvailable() {
        if (unavailable) {
            throw new StoreUnavailableException("in-memory store is simulating an outage");
        }
    }

    /**
     * Pairs a record with the instant it stops being honored (past it,
     * {@code reserve}/{@code find} treat the key as absent) and the fence
     * token that reservation attempt was issued, so a stale primary's
     * {@code complete}/{@code release} can't act on a record it no longer owns.
     */
    private record TimestampedRecord(IdempotencyRecord record, Instant expiresAt, String fenceToken) {
        boolean isExpired(Instant now) {
            return !now.isBefore(expiresAt);
        }
    }
}
