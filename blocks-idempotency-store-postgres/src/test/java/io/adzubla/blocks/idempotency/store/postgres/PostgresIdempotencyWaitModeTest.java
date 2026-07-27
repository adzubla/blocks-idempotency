package io.adzubla.blocks.idempotency.store.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.engine.EngineDecision;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;
import io.adzubla.blocks.idempotency.engine.RejectReason;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@link PostgresIdempotencyStore#await} genuinely blocks on the row's
 * own lock rather than falling back to the generic poll every other store
 * uses: {@code waiterEngine} is configured with a poll interval bigger than
 * {@code WAIT_TIMEOUT} itself, so if {@code await()} were the inherited
 * default it would sleep out almost the entire wait budget before its first
 * {@code find()} check. Resolving promptly instead - close to the primary's
 * real completion delay - proves the native row-lock wait, not a lucky poll,
 * is what unblocked it.
 *
 * <p>{@code waiterStore} is given a short {@code lockTimeout} so its own
 * contended {@code reserve()} gives up quickly and falls through to {@code
 * whenInProgress=WAIT}'s {@code await()} path, rather than resolving via
 * {@code reserve()}'s own blocking first (Slice 018) - the thing this test is
 * actually meant to exercise.
 */
@Testcontainers
class PostgresIdempotencyWaitModeTest {

    private static final Duration LOCK_TTL = Duration.ofSeconds(30);
    private static final Duration RESPONSE_TTL = Duration.ofSeconds(30);
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration PRIMARY_COMPLETION_DELAY = Duration.ofMillis(300);
    private static final Duration WAITER_RESERVE_LOCK_TIMEOUT = Duration.ofMillis(100);
    private static final Duration IMPLAUSIBLY_LONG_POLL_INTERVAL = Duration.ofSeconds(10);

    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private static HikariDataSource dataSource;
    private static DataSourceTransactionManager transactionManager;

    private PostgresIdempotencyStore primaryStore;
    private PostgresIdempotencyStore waiterStore;
    private IdempotencyEngine primaryEngine;
    private IdempotencyEngine waiterEngine;

    @BeforeAll
    static void migrate() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        dataSource.setMaximumPoolSize(4);
        transactionManager = new DataSourceTransactionManager(dataSource);

        Flyway.configure().dataSource(dataSource).load().migrate();
    }

    @AfterAll
    static void closeDataSource() {
        dataSource.close();
    }

    @BeforeEach
    void setUp() {
        primaryStore = new PostgresIdempotencyStore(new JdbcTemplate(dataSource), transactionManager, new ObjectMapper(), LOCK_TTL);
        waiterStore = new PostgresIdempotencyStore(new JdbcTemplate(dataSource), transactionManager, new ObjectMapper(), WAITER_RESERVE_LOCK_TIMEOUT);
        primaryEngine = new IdempotencyEngine(primaryStore);
        waiterEngine = new IdempotencyEngine(waiterStore, IMPLAUSIBLY_LONG_POLL_INTERVAL, Duration.ZERO);
    }

    @AfterEach
    void cleanUpDanglingReservations() {
        primaryStore.abandonDanglingReservationForTests();
        waiterStore.abandonDanglingReservationForTests();
    }

    @Test
    void aWaitModeCallerObservesTheRowsLockReleaseRatherThanPolling() throws Exception {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "wait-native-1");
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());
        CountDownLatch primaryReserved = new CountDownLatch(1);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Void> primary = executor.submit(() -> {
                EngineDecision decision = primaryEngine.before(key, "fp", LOCK_TTL, OnStoreFailure.OPEN, WhenInProgress.REJECT, WAIT_TIMEOUT);
                String fenceToken = ((EngineDecision.Proceed) decision).fenceToken();
                primaryReserved.countDown();
                Thread.sleep(PRIMARY_COMPLETION_DELAY.toMillis());
                primaryEngine.complete(key, fenceToken, response, RESPONSE_TTL);
                return null;
            });
            assertThat(primaryReserved.await(5, TimeUnit.SECONDS)).isTrue();

            long start = System.nanoTime();
            EngineDecision waiterDecision = waiterEngine.before(key, "fp", LOCK_TTL, OnStoreFailure.OPEN, WhenInProgress.WAIT, WAIT_TIMEOUT);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

            primary.get(5, TimeUnit.SECONDS);

            assertThat(waiterDecision).isInstanceOf(EngineDecision.Replay.class);
            assertThat(((EngineDecision.Replay) waiterDecision).response()).isEqualTo(response);
            // Resolved close to the primary's own completion delay, nowhere
            // near the implausibly long poll interval (which the default
            // polling await() would have slept through first) - proves the
            // row lock, not a poll, is what unblocked this.
            assertThat(elapsed).isLessThan(Duration.ofSeconds(2));
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void aWaitModeCallerObservesAPrimarysRollbackAsReleased() throws Exception {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "wait-native-2");
        CountDownLatch primaryReserved = new CountDownLatch(1);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Void> primary = executor.submit(() -> {
                EngineDecision decision = primaryEngine.before(key, "fp", LOCK_TTL, OnStoreFailure.OPEN, WhenInProgress.REJECT, WAIT_TIMEOUT);
                String fenceToken = ((EngineDecision.Proceed) decision).fenceToken();
                primaryReserved.countDown();
                Thread.sleep(PRIMARY_COMPLETION_DELAY.toMillis());
                primaryEngine.release(key, fenceToken);
                return null;
            });
            assertThat(primaryReserved.await(5, TimeUnit.SECONDS)).isTrue();

            EngineDecision waiterDecision = waiterEngine.before(key, "fp", LOCK_TTL, OnStoreFailure.OPEN, WhenInProgress.WAIT, WAIT_TIMEOUT);
            primary.get(5, TimeUnit.SECONDS);

            assertThat(waiterDecision).isInstanceOf(EngineDecision.Reject.class);
            assertThat(((EngineDecision.Reject) waiterDecision).reason()).isEqualTo(RejectReason.RELEASED);
        } finally {
            executor.shutdown();
        }
    }
}
