package io.adzubla.blocks.idempotency.store;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.IdempotencyRecord;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The {@link IdempotencyStore} contract (see ADR 0001), defined once and run
 * against every implementation: {@link InMemoryIdempotencyStore} here,
 * {@code RedisIdempotencyStore} and {@code PostgresIdempotencyStore} in their
 * own modules, each supplying a {@link #createStore()} backed by
 * Testcontainers (Slice 015/017).
 *
 * <p>TTL tests use a short, real duration rather than an injectable
 * {@link java.time.Clock} - not every implementation can be driven by a fake
 * clock (Redis/Postgres expire on their own server-side clock), so this keeps
 * the suite genuinely store-agnostic. Expiry is judged against each store's
 * wall clock, which can step (even backwards - observed on WSL2) relative to
 * the monotonic clock {@code Thread.sleep} uses, so the suite never assumes
 * "slept past the TTL" means "expired": {@link #awaitExpiry} polls until the
 * store itself reports the key absent, bounded by a monotonic deadline. Any
 * reservation whose expiry is <em>not</em> under test uses {@link #STABLE_TTL}
 * so a forward clock step can't expire it mid-test.
 */
public abstract class IdempotencyStoreContractTest {

    /** Short TTL for the record whose expiry a test is exercising. */
    private static final Duration TTL = Duration.ofMillis(150);
    /** TTL for every record that must stay live for the whole test. */
    private static final Duration STABLE_TTL = Duration.ofSeconds(60);
    private static final Duration EXPIRY_DEADLINE = Duration.ofSeconds(15);
    private static final Duration EXPIRY_POLL_INTERVAL = Duration.ofMillis(20);

    private static final Duration AWAIT_RESERVE_TTL = STABLE_TTL;
    private static final Duration AWAIT_WAIT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration AWAIT_TIMEOUT_BUDGET = Duration.ofMillis(150);
    private static final Duration AWAIT_POLL_INTERVAL = Duration.ofMillis(20);
    private static final Duration AWAIT_POLL_JITTER = Duration.ofMillis(10);
    private static final Duration PRIMARY_ACTION_DELAY = Duration.ofMillis(200);
    private static final long AWAIT_JOIN_TIMEOUT_MILLIS = 15_000;

    private IdempotencyStore store;

    /** A fresh store for each test - implementations must start with no keys reserved. */
    protected abstract IdempotencyStore createStore();

    @BeforeEach
    void setUpStore() {
        store = createStore();
    }

    @Test
    void aFreshKeyIsReserved() {
        ReservationResult result = store.reserve(key("k1"), "fp", STABLE_TTL);

        assertThat(result.outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
        assertThat(result.fenceToken()).isPresent();
    }

    @Test
    void findReturnsTheInProgressRecordBeforeCompletion() {
        EffectiveKey key = key("k2");
        store.reserve(key, "fp", STABLE_TTL);

        IdempotencyRecord record = store.find(key).orElseThrow();

        assertThat(record.state()).isEqualTo(RecordState.IN_PROGRESS);
        assertThat(record.fingerprint()).isEqualTo("fp");
    }

    @Test
    void aRepeatReservationOfAnInProgressKeyReturnsTheExistingRecord() {
        EffectiveKey key = key("k3");
        store.reserve(key, "fp", STABLE_TTL);

        ReservationResult repeat = store.reserve(key, "fp", STABLE_TTL);

        assertThat(repeat.outcome()).isEqualTo(ReservationResult.Outcome.EXISTS);
        assertThat(repeat.existing().orElseThrow().state()).isEqualTo(RecordState.IN_PROGRESS);
    }

    @Test
    void aCompletedRecordIsRetrievableViaFindAndViaARepeatReservation() {
        EffectiveKey key = key("k4");
        String fenceToken = store.reserve(key, "fp", STABLE_TTL).fenceToken().orElseThrow();
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());

        store.complete(key, fenceToken, response, STABLE_TTL);

        assertThat(store.find(key).orElseThrow().response()).isEqualTo(response);
        ReservationResult repeat = store.reserve(key, "fp", STABLE_TTL);
        assertThat(repeat.outcome()).isEqualTo(ReservationResult.Outcome.EXISTS);
        assertThat(repeat.existing().orElseThrow().response()).isEqualTo(response);
    }

    @Test
    void aRecordCompletedWithTheEmptySentinelStaysEmptyAcrossARoundTrip() {
        // Messaging completes with CachedResponse.empty(); the engine tells that routine
        // duplicate apart from an unreplayable HTTP response by CachedResponse.isEmpty(),
        // so every store must hand the sentinel back unchanged (issue 068).
        EffectiveKey key = key("k4-empty");
        String fenceToken = store.reserve(key, "fp", STABLE_TTL).fenceToken().orElseThrow();

        store.complete(key, fenceToken, CachedResponse.empty(), STABLE_TTL);

        CachedResponse found = store.find(key).orElseThrow().response();
        assertThat(found.isEmpty()).isTrue();
        assertThat(store.reserve(key, "fp", STABLE_TTL).existing().orElseThrow().response().isEmpty()).isTrue();
    }

    @Test
    void releaseFreesTheKeyForAFreshReservation() {
        EffectiveKey key = key("k5");
        String fenceToken = store.reserve(key, "fp", STABLE_TTL).fenceToken().orElseThrow();

        store.release(key, fenceToken);

        assertThat(store.reserve(key, "fp", STABLE_TTL).outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
    }

    @Test
    void theFingerprintCapturedAtReserveIsRetainedThroughComplete() {
        EffectiveKey key = key("k6");
        String fenceToken = store.reserve(key, "distinctive-fp", STABLE_TTL).fenceToken().orElseThrow();

        store.complete(key, fenceToken, new CachedResponse(201, Map.of(), "{}".getBytes()), STABLE_TTL);

        assertThat(store.find(key).orElseThrow().fingerprint()).isEqualTo("distinctive-fp");
    }

    @Test
    void anInProgressRecordPastItsLockTtlIsTreatedAsAbsent() throws InterruptedException {
        EffectiveKey key = key("k7");
        store.reserve(key, "fp", TTL);

        awaitExpiry(key);

        assertThat(store.reserve(key, "fp", STABLE_TTL).outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
    }

    @Test
    void aCompletedRecordPastItsResponseTtlIsTreatedAsAbsent() throws InterruptedException {
        EffectiveKey key = key("k8");
        String fenceToken = store.reserve(key, "fp", STABLE_TTL).fenceToken().orElseThrow();
        store.complete(key, fenceToken, new CachedResponse(201, Map.of(), "{}".getBytes()), TTL);

        awaitExpiry(key);

        assertThat(store.reserve(key, "fp", STABLE_TTL).outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
    }

    @Test
    void completeIsANoOpWhenTheFenceTokenNoLongerMatches() throws InterruptedException {
        EffectiveKey key = key("k9");
        String staleToken = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();

        awaitExpiry(key);
        // a fresh caller supersedes the expired reservation
        assertThat(store.reserve(key, "fp", STABLE_TTL).outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);

        CachedResponse staleResponse = new CachedResponse(201, Map.of(), "{\"id\":\"stale\"}".getBytes());
        store.complete(key, staleToken, staleResponse, STABLE_TTL);

        assertThat(store.find(key).orElseThrow().state()).isEqualTo(RecordState.IN_PROGRESS);
    }

    @Test
    void releaseIsANoOpWhenTheFenceTokenNoLongerMatches() throws InterruptedException {
        EffectiveKey key = key("k10");
        String staleToken = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();

        awaitExpiry(key);
        // a fresh caller supersedes the expired reservation
        assertThat(store.reserve(key, "fp", STABLE_TTL).outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);

        store.release(key, staleToken);

        assertThat(store.find(key).orElseThrow().state()).isEqualTo(RecordState.IN_PROGRESS);
    }

    /**
     * Blocks until the store itself reports {@code key} absent - i.e. until
     * its own clock says the TTL has passed - failing only if that hasn't
     * happened by a generous deadline measured on the monotonic clock. Uses
     * {@code find} (read-only) so polling can't disturb the record under test.
     */
    private void awaitExpiry(EffectiveKey key) throws InterruptedException {
        long deadline = System.nanoTime() + EXPIRY_DEADLINE.toNanos();
        while (store.find(key).isPresent()) {
            if (System.nanoTime() - deadline > 0) {
                fail("%s still present %s after it should have expired", key, EXPIRY_DEADLINE);
            }
            Thread.sleep(EXPIRY_POLL_INTERVAL.toMillis());
        }
    }

    /**
     * These three exercise {@code await} (ADR 0002 WAIT mode) - deliberately
     * run on a separate thread from the {@code reserve} call, never the same
     * one: {@code PostgresIdempotencyStore} binds a reservation's transaction
     * to the reserving thread, so an {@code await} sharing that thread would
     * silently join the reservation's own still-open transaction instead of
     * genuinely blocking on it, defeating the point of the test.
     */
    @Test
    void awaitResolvesWithTheCompletedRecordOnceThePrimaryCompletes() throws InterruptedException {
        EffectiveKey key = key("k11");
        String fenceToken = store.reserve(key, "fp", AWAIT_RESERVE_TTL).fenceToken().orElseThrow();
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());

        AtomicReference<Optional<IdempotencyRecord>> awaitResult = new AtomicReference<>();
        Thread waiter = new Thread(() -> awaitResult.set(store.await(key, AWAIT_WAIT_TIMEOUT, AWAIT_POLL_INTERVAL, AWAIT_POLL_JITTER)));
        waiter.start();

        Thread.sleep(PRIMARY_ACTION_DELAY.toMillis());
        store.complete(key, fenceToken, response, AWAIT_RESERVE_TTL);
        waiter.join(AWAIT_JOIN_TIMEOUT_MILLIS);

        assertThat(waiter.isAlive()).isFalse();
        assertThat(awaitResult.get()).isPresent();
        assertThat(awaitResult.get().orElseThrow().state()).isEqualTo(RecordState.COMPLETED);
        assertThat(awaitResult.get().orElseThrow().response()).isEqualTo(response);
    }

    @Test
    void awaitResolvesEmptyWhenThePrimaryReleasesTheKeyWhileWaiting() throws InterruptedException {
        EffectiveKey key = key("k12");
        String fenceToken = store.reserve(key, "fp", AWAIT_RESERVE_TTL).fenceToken().orElseThrow();

        AtomicReference<Optional<IdempotencyRecord>> awaitResult = new AtomicReference<>();
        Thread waiter = new Thread(() -> awaitResult.set(store.await(key, AWAIT_WAIT_TIMEOUT, AWAIT_POLL_INTERVAL, AWAIT_POLL_JITTER)));
        waiter.start();

        Thread.sleep(PRIMARY_ACTION_DELAY.toMillis());
        store.release(key, fenceToken);
        waiter.join(AWAIT_JOIN_TIMEOUT_MILLIS);

        assertThat(waiter.isAlive()).isFalse();
        assertThat(awaitResult.get()).isEmpty();
    }

    @Test
    void awaitResolvesWithTheStillInProgressRecordWhenWaitTimeoutElapsesFirst() throws InterruptedException {
        EffectiveKey key = key("k13");
        store.reserve(key, "fp", AWAIT_RESERVE_TTL);

        AtomicReference<Optional<IdempotencyRecord>> awaitResult = new AtomicReference<>();
        Thread waiter = new Thread(() -> awaitResult.set(store.await(key, AWAIT_TIMEOUT_BUDGET, AWAIT_POLL_INTERVAL, AWAIT_POLL_JITTER)));
        waiter.start();
        waiter.join(AWAIT_JOIN_TIMEOUT_MILLIS);

        assertThat(waiter.isAlive()).isFalse();
        assertThat(awaitResult.get()).isPresent();
        assertThat(awaitResult.get().orElseThrow().state()).isEqualTo(RecordState.IN_PROGRESS);
    }

    private static EffectiveKey key(String value) {
        return new EffectiveKey("/orders", "POST", "", value);
    }
}
