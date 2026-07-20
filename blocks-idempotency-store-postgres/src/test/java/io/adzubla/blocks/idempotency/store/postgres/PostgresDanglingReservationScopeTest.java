package io.adzubla.blocks.idempotency.store.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Documents a robustness gap
 * (docs/issues/033-postgres-dangling-reservation-scope.md): the winning
 * {@code reserve()} leaves its transaction open and thread-bound via a
 * {@code ThreadLocal} reservation scope, relying on {@code complete()}/{@code
 * release()} to resolve it. If a reservation is ever left unresolved on a
 * pooled thread (a thread-hopping/async handler - explicitly "not yet
 * supported", or an interceptor that never ran), the stale scope survives onto
 * the next request that reuses the thread.
 *
 * <p>This test drives that state directly at the store level: reserve key A and
 * leave it dangling, then - simulating the reused thread handling a fresh
 * request - reserve a different key B and complete B. Because B's
 * {@code reserve} joins A's still-open transaction and {@code bindScope} keeps
 * the outer transaction, completing B commits A's dangling reservation as a
 * side effect: A leaks as a committed {@code IN_PROGRESS} row that will block
 * genuine retries until it expires, even though A's request was never resolved.
 *
 * <p>The trigger (unresolved reservation surviving a thread) is currently
 * out-of-contract (ADR 0003: async not supported), so this is filed as
 * defense-in-depth hardening, not a bug in the supported synchronous model.
 * {@code @Disabled}; enabling it fails, demonstrating the leak.
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

        Flyway.configure().dataSource(dataSource).load().migrate();
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
    @Disabled("Documents robustness gap: docs/issues/033-postgres-dangling-reservation-scope.md; remove @Disabled to reproduce")
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
    }
}
