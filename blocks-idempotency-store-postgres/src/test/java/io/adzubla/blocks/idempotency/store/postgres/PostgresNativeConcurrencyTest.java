package io.adzubla.blocks.idempotency.store.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
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
 * Genuine two-thread, two-connection proof of ADR 0001/0002's Postgres
 * concurrency divergence (Slice 018): a second {@code reserve()} on the same
 * key blocks on the reservation row's own lock rather than going through the
 * core's polling {@code find()} loop, and resolves according to what the
 * primary does with its transaction - commit (replay), rollback
 * (self-promotion), or neither within {@code lock_timeout} (bounded reject).
 *
 * <p>Deliberately at the store level, not through {@code IdempotencyEngine}/
 * MockMvc: this is the one behavior that's specific to how {@link
 * PostgresIdempotencyStore#reserve} itself blocks, and pinning it here keeps
 * the proof precise (latch-driven, not sleep-and-hope) and fast. {@code
 * PostgresIdempotencyEndToEndTest} separately proves the full-stack "exactly
 * once, not zero or twice" outcome through real concurrent HTTP requests.
 */
@Testcontainers
class PostgresNativeConcurrencyTest {

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration DEFAULT_LOCK_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration SHORT_LOCK_TIMEOUT = Duration.ofMillis(300);

    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private static HikariDataSource dataSource;
    private static DataSourceTransactionManager transactionManager;

    private PostgresIdempotencyStore store;

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
        store = new PostgresIdempotencyStore(new JdbcTemplate(dataSource), transactionManager, new ObjectMapper(), DEFAULT_LOCK_TIMEOUT);
    }

    @Test
    void aConcurrentReserveBlocksRatherThanReturningImmediately() throws Exception {
        EffectiveKey key = key("blocks-1");
        CountDownLatch primaryReserved = new CountDownLatch(1);
        CountDownLatch releasePrimary = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<String> primary = executor.submit(() -> {
                String fenceToken = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();
                primaryReserved.countDown();
                releasePrimary.await(5, TimeUnit.SECONDS);
                store.complete(key, fenceToken, new CachedResponse(201, Map.of(), "{}".getBytes()), TTL);
                return fenceToken;
            });
            assertThat(primaryReserved.await(5, TimeUnit.SECONDS)).isTrue();

            Future<ReservationResult> waiter = executor.submit(() -> store.reserve(key, "fp", TTL));

            // The primary is deliberately still holding the reservation open -
            // if the waiter resolved via anything other than blocking on the
            // row's lock (e.g. polling find() and giving up), it would have
            // returned well before this.
            Thread.sleep(300);
            assertThat(waiter.isDone()).isFalse();

            releasePrimary.countDown();
            primary.get(5, TimeUnit.SECONDS);
            ReservationResult result = waiter.get(5, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(ReservationResult.Outcome.EXISTS);
            assertThat(result.existing().orElseThrow().isCompleted()).isTrue();
        }
    }

    @Test
    void whenThePrimaryCommitsTheBlockedWaiterObservesTheCompletedResponse() throws Exception {
        EffectiveKey key = key("commit-1");
        CountDownLatch primaryReserved = new CountDownLatch(1);
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<Void> primary = executor.submit(() -> {
                String fenceToken = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();
                primaryReserved.countDown();
                Thread.sleep(200);
                store.complete(key, fenceToken, response, TTL);
                return null;
            });
            assertThat(primaryReserved.await(5, TimeUnit.SECONDS)).isTrue();

            ReservationResult waiterResult = executor.submit(() -> store.reserve(key, "fp", TTL)).get(5, TimeUnit.SECONDS);
            primary.get(5, TimeUnit.SECONDS);

            assertThat(waiterResult.outcome()).isEqualTo(ReservationResult.Outcome.EXISTS);
            assertThat(waiterResult.existing().orElseThrow().response()).isEqualTo(response);
        }
    }

    @Test
    void whenThePrimaryRollsBackTheBlockedWaiterSelfPromotesToExecutor() throws Exception {
        EffectiveKey key = key("rollback-1");
        CountDownLatch primaryReserved = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<Void> primary = executor.submit(() -> {
                String fenceToken = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();
                primaryReserved.countDown();
                Thread.sleep(200);
                store.release(key, fenceToken); // simulated crash/error - rolls back
                return null;
            });
            assertThat(primaryReserved.await(5, TimeUnit.SECONDS)).isTrue();

            // The completion, if any, must run on the SAME thread as the reserve()
            // that won it - PostgresIdempotencyStore's transaction binding is
            // thread-local (ADR 0003), exactly like a real request thread running
            // preHandle -> handler -> afterCompletion.
            Future<ReservationResult> waiter = executor.submit(() -> {
                ReservationResult result = store.reserve(key, "fp", TTL);
                if (result.outcome() == ReservationResult.Outcome.RESERVED) {
                    store.complete(key, result.fenceToken().orElseThrow(), new CachedResponse(201, Map.of(), "{}".getBytes()), TTL);
                }
                return result;
            });

            ReservationResult waiterResult = waiter.get(5, TimeUnit.SECONDS);
            primary.get(5, TimeUnit.SECONDS);

            // Self-promotion: the waiter, not the (rolled-back) primary, ends up
            // owning and completing the reservation - exactly once.
            assertThat(waiterResult.outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
            assertThat(store.find(key).orElseThrow().isCompleted()).isTrue();
        }
    }

    @Test
    void aWaiterGivesUpAtLockTimeoutRatherThanBlockingForever() throws Exception {
        PostgresIdempotencyStore boundedStore = new PostgresIdempotencyStore(
                new JdbcTemplate(dataSource), transactionManager, new ObjectMapper(), SHORT_LOCK_TIMEOUT);
        EffectiveKey key = key("timeout-1");
        CountDownLatch primaryReserved = new CountDownLatch(1);
        CountDownLatch releasePrimary = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<Void> primary = executor.submit(() -> {
                String fenceToken = boundedStore.reserve(key, "fp", TTL).fenceToken().orElseThrow();
                primaryReserved.countDown();
                // Deliberately never resolves within the waiter's lock_timeout -
                // simulates a live but stuck/slow holder, not a crash (a crash
                // drops the connection, which Postgres rolls back on its own -
                // the self-promotion test above already covers that path).
                releasePrimary.await(5, TimeUnit.SECONDS);
                boundedStore.release(key, fenceToken);
                return null;
            });
            assertThat(primaryReserved.await(5, TimeUnit.SECONDS)).isTrue();

            long start = System.nanoTime();
            ReservationResult waiterResult = executor.submit(() -> boundedStore.reserve(key, "fp", TTL)).get(5, TimeUnit.SECONDS);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

            assertThat(waiterResult.outcome()).isEqualTo(ReservationResult.Outcome.EXISTS);
            assertThat(waiterResult.existing().orElseThrow().state()).isEqualTo(RecordState.IN_PROGRESS);
            // Bounded by lock_timeout (300ms), nowhere near the primary's 5s hold.
            assertThat(elapsed).isLessThan(Duration.ofSeconds(2));

            releasePrimary.countDown();
            primary.get(5, TimeUnit.SECONDS);
        }
    }

    private static EffectiveKey key(String value) {
        return new EffectiveKey("POST", "/orders", "", value);
    }
}
