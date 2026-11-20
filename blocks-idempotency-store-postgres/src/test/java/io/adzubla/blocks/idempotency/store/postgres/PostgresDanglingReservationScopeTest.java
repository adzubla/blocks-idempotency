package io.adzubla.blocks.idempotency.store.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * docs/issues/033-postgres-dangling-reservation-scope.md: the winning
 * {@code reserve()} leaves its transaction open and thread-bound, relying on
 * {@code complete()}/{@code release()} to resolve it. If a reservation is ever
 * left unresolved on a pooled thread (an async/thread-hopping handler, or a
 * path where the interceptor never ran), the next {@code reserve()} for a
 * different key on that thread must not join it - otherwise completing the
 * fresh reservation would commit the dangling one as a side effect, leaking a
 * committed {@code IN_PROGRESS} row that blocks genuine retries until it
 * expires. The store rolls the dangling reservation back instead.
 */
@Testcontainers
class PostgresDanglingReservationScopeTest {

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
        transactionManager = new DataSourceTransactionManager(dataSource);

        PostgresTestSchema.migrate(dataSource);
    }

    @AfterAll
    static void closeDataSource() {
        dataSource.close();
    }

    @AfterEach
    void cleanUpDanglingReservation() {
        // Force-abandon anything the scenario left bound to this test thread.
        store.abandonDanglingReservationForTests();
    }

    @Test
    void completingAFreshReservationDoesNotCommitADanglingOneLeftOnTheSameThread() {
        store = new PostgresIdempotencyStore(new JdbcTemplate(dataSource), transactionManager, new ObjectMapper(), Duration.ofSeconds(2));
        EffectiveKey dangling = new EffectiveKey("/orders", "POST", "", "dangling-A");
        EffectiveKey fresh = new EffectiveKey("/invoices", "POST", "", "fresh-B");

        // Request 1 reserves A and is never resolved (no complete/release).
        store.reserve(dangling, "fpA", Duration.ofSeconds(30));

        // The pooled thread is reused by request 2, which reserves and completes B.
        String tokenB = store.reserve(fresh, "fpB", Duration.ofSeconds(30)).fenceToken().orElseThrow();
        store.complete(fresh, tokenB, new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes()), Duration.ofHours(24));

        // A was never resolved, so it must not have leaked as a committed reservation.
        assertThat(store.find(dangling)).isEmpty();
        assertThat(store.find(fresh).orElseThrow().isCompleted()).isTrue();
        // ...and neither transaction is still bound to the (pooled) thread.
        assertThat(TransactionSynchronizationManager.getResourceMap()).isEmpty();
    }

    @Test
    void aDanglingReservationsKeyIsFreeForAFreshReservationAfterwards() {
        store = new PostgresIdempotencyStore(new JdbcTemplate(dataSource), transactionManager, new ObjectMapper(), Duration.ofSeconds(2));
        EffectiveKey dangling = new EffectiveKey("/orders", "POST", "", "dangling-C");
        EffectiveKey fresh = new EffectiveKey("/invoices", "POST", "", "fresh-D");

        store.reserve(dangling, "fpC", Duration.ofSeconds(30));
        String tokenD = store.reserve(fresh, "fpD", Duration.ofSeconds(30)).fenceToken().orElseThrow();
        store.release(fresh, tokenD);

        // A genuine retry of C is not blocked by the abandoned reservation.
        assertThat(store.reserve(dangling, "fpC", Duration.ofSeconds(30)).outcome())
                .isEqualTo(ReservationResult.Outcome.RESERVED);
    }

    @Test
    void aNestedReservationOfTheSameKeyStillJoinsTheOpenOne() {
        store = new PostgresIdempotencyStore(new JdbcTemplate(dataSource), transactionManager, new ObjectMapper(), Duration.ofSeconds(2));
        EffectiveKey key = new EffectiveKey("/orders", "POST", "", "same-E");
        String token = store.reserve(key, "fpE", Duration.ofSeconds(30)).fenceToken().orElseThrow();

        // Same key on the same thread is not treated as a leak: it sees its own in-progress record...
        assertThat(store.reserve(key, "fpE", Duration.ofSeconds(30)).outcome())
                .isEqualTo(ReservationResult.Outcome.EXISTS);

        // ...and the original reservation is still the one that completes.
        store.complete(key, token, new CachedResponse(201, Map.of(), "{}".getBytes()), Duration.ofHours(24));
        assertThat(store.find(key).orElseThrow().isCompleted()).isTrue();
    }
}
