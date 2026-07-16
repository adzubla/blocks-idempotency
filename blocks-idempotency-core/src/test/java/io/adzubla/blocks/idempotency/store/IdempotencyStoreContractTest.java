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

/**
 * The {@link IdempotencyStore} contract (see ADR 0001), defined once and run
 * against every implementation: {@link InMemoryIdempotencyStore} here,
 * {@code RedisIdempotencyStore} and {@code PostgresIdempotencyStore} in their
 * own modules, each supplying a {@link #createStore()} backed by
 * Testcontainers (Slice 015/017).
 *
 * <p>TTL tests use a short, real duration and a real sleep rather than an
 * injectable {@link java.time.Clock} - not every implementation can be driven
 * by a fake clock (Redis/Postgres expire on their own server-side clock), so
 * this keeps the suite genuinely store-agnostic at the cost of a little
 * wall-clock time per test.
 */
public abstract class IdempotencyStoreContractTest {

    private static final Duration TTL = Duration.ofMillis(150);
    private static final Duration PAST_TTL_SLEEP = TTL.plusMillis(150);

    private static final Duration AWAIT_RESERVE_TTL = Duration.ofSeconds(10);
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
        ReservationResult result = store.reserve(key("k1"), "fp", TTL);

        assertThat(result.outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
        assertThat(result.fenceToken()).isPresent();
    }

    @Test
    void findReturnsTheInProgressRecordBeforeCompletion() {
        EffectiveKey key = key("k2");
        store.reserve(key, "fp", TTL);

        IdempotencyRecord record = store.find(key).orElseThrow();

        assertThat(record.state()).isEqualTo(RecordState.IN_PROGRESS);
        assertThat(record.fingerprint()).isEqualTo("fp");
    }

    @Test
    void aRepeatReservationOfAnInProgressKeyReturnsTheExistingRecord() {
        EffectiveKey key = key("k3");
        store.reserve(key, "fp", TTL);

        ReservationResult repeat = store.reserve(key, "fp", TTL);

        assertThat(repeat.outcome()).isEqualTo(ReservationResult.Outcome.EXISTS);
        assertThat(repeat.existing().orElseThrow().state()).isEqualTo(RecordState.IN_PROGRESS);
    }

    @Test
    void aCompletedRecordIsRetrievableViaFindAndViaARepeatReservation() {
        EffectiveKey key = key("k4");
        String fenceToken = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());

        store.complete(key, fenceToken, response, TTL);

        assertThat(store.find(key).orElseThrow().response()).isEqualTo(response);
        ReservationResult repeat = store.reserve(key, "fp", TTL);
        assertThat(repeat.outcome()).isEqualTo(ReservationResult.Outcome.EXISTS);
        assertThat(repeat.existing().orElseThrow().response()).isEqualTo(response);
    }

    @Test
    void releaseFreesTheKeyForAFreshReservation() {
        EffectiveKey key = key("k5");
        String fenceToken = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();

        store.release(key, fenceToken);

        assertThat(store.reserve(key, "fp", TTL).outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
    }

    @Test
    void theFingerprintCapturedAtReserveIsRetainedThroughComplete() {
        EffectiveKey key = key("k6");
        String fenceToken = store.reserve(key, "distinctive-fp", TTL).fenceToken().orElseThrow();

        store.complete(key, fenceToken, new CachedResponse(201, Map.of(), "{}".getBytes()), TTL);

        assertThat(store.find(key).orElseThrow().fingerprint()).isEqualTo("distinctive-fp");
    }

    @Test
    void anInProgressRecordPastItsLockTtlIsTreatedAsAbsent() throws InterruptedException {
        EffectiveKey key = key("k7");
        store.reserve(key, "fp", TTL);

        Thread.sleep(PAST_TTL_SLEEP.toMillis());

        assertThat(store.find(key)).isEmpty();
        assertThat(store.reserve(key, "fp", TTL).outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
    }

    @Test
    void aCompletedRecordPastItsResponseTtlIsTreatedAsAbsent() throws InterruptedException {
        EffectiveKey key = key("k8");
        String fenceToken = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();
        store.complete(key, fenceToken, new CachedResponse(201, Map.of(), "{}".getBytes()), TTL);

        Thread.sleep(PAST_TTL_SLEEP.toMillis());

        assertThat(store.find(key)).isEmpty();
        assertThat(store.reserve(key, "fp", TTL).outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
    }

    @Test
    void completeIsANoOpWhenTheFenceTokenNoLongerMatches() throws InterruptedException {
        EffectiveKey key = key("k9");
        String staleToken = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();

        Thread.sleep(PAST_TTL_SLEEP.toMillis());
        store.reserve(key, "fp", TTL); // a fresh caller supersedes the expired reservation

        CachedResponse staleResponse = new CachedResponse(201, Map.of(), "{\"id\":\"stale\"}".getBytes());
        store.complete(key, staleToken, staleResponse, TTL);

        assertThat(store.find(key).orElseThrow().state()).isEqualTo(RecordState.IN_PROGRESS);
    }

    @Test
    void releaseIsANoOpWhenTheFenceTokenNoLongerMatches() throws InterruptedException {
        EffectiveKey key = key("k10");
        String staleToken = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();

        Thread.sleep(PAST_TTL_SLEEP.toMillis());
        store.reserve(key, "fp", TTL); // a fresh caller supersedes the expired reservation

        store.release(key, staleToken);

        assertThat(store.find(key).orElseThrow().state()).isEqualTo(RecordState.IN_PROGRESS);
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
        return new EffectiveKey("POST", "/orders", "", value);
    }
}
