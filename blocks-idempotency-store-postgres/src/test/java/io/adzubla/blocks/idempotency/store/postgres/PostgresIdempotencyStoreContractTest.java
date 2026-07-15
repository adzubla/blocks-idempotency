package io.adzubla.blocks.idempotency.store.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.IdempotencyStoreContractTest;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

/**
 * Runs the shared {@link IdempotencyStoreContractTest} suite (Slice 014)
 * against a real Postgres, proving {@link PostgresIdempotencyStore} honors the
 * same {@code reserve}/{@code find}/{@code complete}/{@code release} contract
 * as every other store, on top of a fundamentally different mechanism
 * (transactional native locking, ADR 0003, rather than TTL-based expiry).
 *
 * <p>Several tests in the shared suite deliberately leave a reservation
 * dangling (no terminal {@code complete}/{@code release} call) to simulate a
 * crashed primary. For Postgres that dangling reservation is a genuinely open,
 * thread-bound transaction - since the JUnit test thread is reused across
 * {@code @Test} methods in this class, {@link #cleanUpDanglingReservation()}
 * force-abandons it after every test so it can't bleed into the next one.
 */
@Testcontainers
class PostgresIdempotencyStoreContractTest extends IdempotencyStoreContractTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

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
        store.abandonDanglingReservationForTests();
    }

    @Override
    protected IdempotencyStore createStore() {
        store = new PostgresIdempotencyStore(new JdbcTemplate(dataSource), transactionManager, new ObjectMapper(), Duration.ofSeconds(2));
        return store;
    }
}
