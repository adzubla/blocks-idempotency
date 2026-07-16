package io.adzubla.blocks.idempotency.engine;

import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class IdempotencyEngineTest {

    private final InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
    private final IdempotencyEngine engine = new IdempotencyEngine(store);
    private final Duration lockTtl = Duration.ofSeconds(30);
    private final Duration responseTtl = Duration.ofHours(24);
    private final Duration waitTimeout = Duration.ofSeconds(5);
    private final OnStoreFailure openPosture = OnStoreFailure.OPEN;
    private final WhenInProgress rejectMode = WhenInProgress.REJECT;

    @Test
    void freshKeyProceeds() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-1");

        EngineDecision decision = engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Proceed.class);
        assertThat(((EngineDecision.Proceed) decision).key()).isEqualTo(key);
    }

    @Test
    void completedKeyReplaysTheStoredResponse() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-2");
        String fenceToken = proceedToken(engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse response = new CachedResponse(201, Map.of("Content-Type", java.util.List.of("application/json")), "{\"id\":1}".getBytes());
        engine.complete(key, fenceToken, response, responseTtl);

        EngineDecision decision = engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Replay.class);
        assertThat(((EngineDecision.Replay) decision).response()).isEqualTo(response);
    }

    @Test
    void concurrentDuplicateWithTheSameFingerprintIsRejected() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-3");
        engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        EngineDecision decision = engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Reject.class);
        EngineDecision.Reject reject = (EngineDecision.Reject) decision;
        assertThat(reject.reason()).isEqualTo(RejectReason.IN_PROGRESS);
        assertThat(reject.retryAfter()).isEqualTo(lockTtl);
    }

    @Test
    void differentEffectiveKeysDoNotCollide() {
        EffectiveKey key1 = new EffectiveKey("POST", "/orders", "", "key-4");
        EffectiveKey key2 = new EffectiveKey("POST", "/orders", "", "key-5");

        engine.before(key1, "fp", lockTtl, openPosture, rejectMode, waitTimeout);
        EngineDecision decision = engine.before(key2, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Proceed.class);
    }

    @Test
    void sameKeyDifferentFingerprintAgainstACompletedRecordIsACollision() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-6");
        String fenceToken = proceedToken(engine.before(key, "fp-original", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());
        engine.complete(key, fenceToken, response, responseTtl);

        EngineDecision decision = engine.before(key, "fp-different", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Collision.class);
    }

    @Test
    void sameKeyDifferentFingerprintAgainstAnInProgressRecordIsACollision() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-7");
        engine.before(key, "fp-original", lockTtl, openPosture, rejectMode, waitTimeout);

        EngineDecision decision = engine.before(key, "fp-different", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Collision.class);
    }

    @Test
    void sameKeySameFingerprintStillReplaysAfterCompletion() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-8");
        String fenceToken = proceedToken(engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());
        engine.complete(key, fenceToken, response, responseTtl);

        EngineDecision decision = engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Replay.class);
    }

    @Test
    void releaseFreesTheKeyForAFreshReservation() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-9");
        String fenceToken = proceedToken(engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));

        engine.release(key, fenceToken);

        EngineDecision decision = engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Proceed.class);
    }

    @Test
    void completedKeyIsNotAffectedByAnUnrelatedRelease() {
        EffectiveKey completedKey = new EffectiveKey("POST", "/orders", "", "key-10");
        String fenceToken = proceedToken(engine.before(completedKey, "fp", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());
        engine.complete(completedKey, fenceToken, response, responseTtl);

        // Never reserved, so any token is as good as none - release() must be a no-op either way.
        EffectiveKey otherKey = new EffectiveKey("POST", "/orders", "", "key-11");
        engine.release(otherKey, "unused");

        EngineDecision decision = engine.before(completedKey, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Replay.class);
    }

    @Test
    void unavailableStoreWithFailOpenProceedsUnprotected() {
        store.setUnavailable(true);
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-12");

        EngineDecision decision = engine.before(key, "fp", lockTtl, OnStoreFailure.OPEN, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.ProceedUnprotected.class);
    }

    @Test
    void unavailableStoreWithFailClosedRejects() {
        store.setUnavailable(true);
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-13");

        EngineDecision decision = engine.before(key, "fp", lockTtl, OnStoreFailure.CLOSED, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.FailClosed.class);
    }

    @Test
    void storeRecoveringAfterAnOutageProceedsNormallyAgain() {
        store.setUnavailable(true);
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-14");
        engine.before(key, "fp", lockTtl, OnStoreFailure.OPEN, rejectMode, waitTimeout);

        store.setUnavailable(false);
        EngineDecision decision = engine.before(key, "fp", lockTtl, OnStoreFailure.OPEN, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Proceed.class);
    }

    @Test
    void completeSwallowsAMidRequestStoreOutageInsteadOfPropagating() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-15");
        String fenceToken = proceedToken(engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());
        store.setUnavailable(true);

        assertThatCode(() -> engine.complete(key, fenceToken, response, responseTtl)).doesNotThrowAnyException();
    }

    @Test
    void releaseSwallowsAMidRequestStoreOutageInsteadOfPropagating() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-16");
        String fenceToken = proceedToken(engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));
        store.setUnavailable(true);

        assertThatCode(() -> engine.release(key, fenceToken)).doesNotThrowAnyException();
    }

    @Test
    void completedKeyStillReplaysJustBeforeTheTtlExpires() {
        MutableClock clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"), ZoneOffset.UTC);
        InMemoryIdempotencyStore clockedStore = new InMemoryIdempotencyStore(clock);
        IdempotencyEngine clockedEngine = new IdempotencyEngine(clockedStore);
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-17");
        String fenceToken = proceedToken(clockedEngine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());
        clockedEngine.complete(key, fenceToken, response, responseTtl);

        clock.advance(responseTtl.minusSeconds(1));
        EngineDecision decision = clockedEngine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Replay.class);
    }

    @Test
    void completedKeyReExecutesAsABrandNewKeyOnceTheTtlExpires() {
        MutableClock clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"), ZoneOffset.UTC);
        InMemoryIdempotencyStore clockedStore = new InMemoryIdempotencyStore(clock);
        IdempotencyEngine clockedEngine = new IdempotencyEngine(clockedStore);
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-18");
        String fenceToken = proceedToken(clockedEngine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());
        clockedEngine.complete(key, fenceToken, response, responseTtl);

        clock.advance(responseTtl);
        EngineDecision decision = clockedEngine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Proceed.class);
    }

    @Test
    void expiredKeyProceedsFreshEvenWithADifferentFingerprintNoCollision() {
        MutableClock clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"), ZoneOffset.UTC);
        InMemoryIdempotencyStore clockedStore = new InMemoryIdempotencyStore(clock);
        IdempotencyEngine clockedEngine = new IdempotencyEngine(clockedStore);
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-19");
        String fenceToken = proceedToken(clockedEngine.before(key, "fp-original", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());
        clockedEngine.complete(key, fenceToken, response, responseTtl);

        clock.advance(responseTtl);
        EngineDecision decision = clockedEngine.before(key, "fp-different", lockTtl, openPosture, rejectMode, waitTimeout);

        // Expiry is indistinguishable from a brand-new key: no collision, even
        // though the (expired) fingerprint on file differs.
        assertThat(decision).isInstanceOf(EngineDecision.Proceed.class);
    }

    @Test
    void inProgressKeyPastItsLockTtlAlsoBehavesAsABrandNewKey() {
        MutableClock clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"), ZoneOffset.UTC);
        InMemoryIdempotencyStore clockedStore = new InMemoryIdempotencyStore(clock);
        IdempotencyEngine clockedEngine = new IdempotencyEngine(clockedStore);
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-20");
        clockedEngine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        // No complete()/release() - simulates a crashed primary; its lock
        // eventually expires (ADR 0002: "Crashed (lock-ttl expired) | key gone").
        clock.advance(lockTtl);
        EngineDecision decision = clockedEngine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Proceed.class);
    }

    // The stale-writer fencing behavior these two tests covered (complete()/
    // release() no-op when the fence token no longer matches) is now part of
    // the shared IdempotencyStoreContractTest suite - see
    // io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStoreContractTest.

    @Test
    void waitReplaysOncePrimaryCompletes() {
        IdempotencyEngine waitEngine = new IdempotencyEngine(store, Duration.ofMillis(15), Duration.ofMillis(10));
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-23");
        String fenceToken = proceedToken(engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());

        CompletableFuture.runAsync(() -> {
            sleepQuietly(Duration.ofMillis(40));
            engine.complete(key, fenceToken, response, responseTtl);
        });

        EngineDecision decision = waitEngine.before(key, "fp", lockTtl, openPosture, WhenInProgress.WAIT, Duration.ofMillis(300));

        assertThat(decision).isInstanceOf(EngineDecision.Replay.class);
        assertThat(((EngineDecision.Replay) decision).response()).isEqualTo(response);
    }

    @Test
    void waitReturnsRejectWithReleasedReasonWhenTheKeyDisappears() {
        IdempotencyEngine waitEngine = new IdempotencyEngine(store, Duration.ofMillis(15), Duration.ofMillis(10));
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-24");
        String fenceToken = proceedToken(engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));

        CompletableFuture.runAsync(() -> {
            sleepQuietly(Duration.ofMillis(40));
            engine.release(key, fenceToken);
        });

        EngineDecision decision = waitEngine.before(key, "fp", lockTtl, openPosture, WhenInProgress.WAIT, Duration.ofMillis(300));

        assertThat(decision).isInstanceOf(EngineDecision.Reject.class);
        assertThat(((EngineDecision.Reject) decision).reason()).isEqualTo(RejectReason.RELEASED);
    }

    @Test
    void waitReturnsRejectWithTimeoutReasonAndIsBoundedByWaitTimeout() {
        IdempotencyEngine waitEngine = new IdempotencyEngine(store, Duration.ofMillis(15), Duration.ofMillis(5));
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-25");
        engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);
        Duration shortWaitTimeout = Duration.ofMillis(80);

        Instant start = Instant.now();
        EngineDecision decision = waitEngine.before(key, "fp", lockTtl, openPosture, WhenInProgress.WAIT, shortWaitTimeout);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(decision).isInstanceOf(EngineDecision.Reject.class);
        assertThat(((EngineDecision.Reject) decision).reason()).isEqualTo(RejectReason.TIMEOUT);
        // Tightly bounded: each poll sleep is capped to the remaining budget,
        // so this shouldn't overshoot waitTimeout by more than scheduling slack.
        assertThat(elapsed).isGreaterThanOrEqualTo(shortWaitTimeout);
        assertThat(elapsed).isLessThan(shortWaitTimeout.plusMillis(60));
    }

    @Test
    void waitAppliesTheStoreFailurePostureIfTheStoreGoesDownMidPoll() {
        IdempotencyEngine waitEngine = new IdempotencyEngine(store, Duration.ofMillis(15), Duration.ofMillis(10));
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-28");
        engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        CompletableFuture.runAsync(() -> {
            sleepQuietly(Duration.ofMillis(40));
            store.setUnavailable(true);
        });

        EngineDecision decision = waitEngine.before(key, "fp", lockTtl, OnStoreFailure.CLOSED, WhenInProgress.WAIT, Duration.ofMillis(300));

        assertThat(decision).isInstanceOf(EngineDecision.FailClosed.class);
    }

    @Test
    void completedWithoutBodyIsReportedAsResponseUnavailable() {
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-26");
        String fenceToken = proceedToken(engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse notReplayable = new CachedResponse(201, Map.of(), null);
        engine.complete(key, fenceToken, notReplayable, responseTtl);

        EngineDecision decision = engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout);

        assertThat(decision).isInstanceOf(EngineDecision.Unavailable.class);
    }

    @Test
    void waiterAlsoReportsResponseUnavailableWhenThePrimaryCompletesWithoutABody() {
        IdempotencyEngine waitEngine = new IdempotencyEngine(store, Duration.ofMillis(15), Duration.ofMillis(10));
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-27");
        String fenceToken = proceedToken(engine.before(key, "fp", lockTtl, openPosture, rejectMode, waitTimeout));
        CachedResponse notReplayable = new CachedResponse(201, Map.of(), null);

        CompletableFuture.runAsync(() -> {
            sleepQuietly(Duration.ofMillis(40));
            engine.complete(key, fenceToken, notReplayable, responseTtl);
        });

        EngineDecision decision = waitEngine.before(key, "fp", lockTtl, openPosture, WhenInProgress.WAIT, Duration.ofMillis(300));

        assertThat(decision).isInstanceOf(EngineDecision.Unavailable.class);
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static String proceedToken(EngineDecision decision) {
        return ((EngineDecision.Proceed) decision).fenceToken();
    }

    /** A {@link Clock} tests can advance, so TTL expiry is deterministic without sleeping. */
    private static final class MutableClock extends Clock {
        private Instant instant;
        private final ZoneId zone;

        MutableClock(Instant instant, ZoneId zone) {
            this.instant = instant;
            this.zone = zone;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return new MutableClock(instant, zone);
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
