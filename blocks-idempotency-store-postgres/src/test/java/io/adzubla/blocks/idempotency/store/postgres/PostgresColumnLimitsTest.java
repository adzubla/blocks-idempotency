package io.adzubla.blocks.idempotency.store.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
 * docs/issues/061-postgres-schema-column-limits-raw-500.md: the effective-key
 * columns are {@code VARCHAR(512)}/{@code VARCHAR(255)}, but the application
 * accepts longer routes, principals, listener ids and (with a raised {@code
 * idempotency.key.max-length}) key values. A component that doesn't fit is
 * stored as a digest of itself, so it inserts instead of failing with SQLState
 * 22001 (a raw 500 that bypasses {@code onStoreFailure}).
 */
@Testcontainers
class PostgresColumnLimitsTest {

    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private static final Duration TTL = Duration.ofMinutes(5);

    private static HikariDataSource dataSource;
    private static DataSourceTransactionManager transactionManager;

    private JdbcTemplate jdbc;
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

    @BeforeEach
    void setUp() {
        jdbc = new JdbcTemplate(dataSource);
        jdbc.update("DELETE FROM idempotency_record");
        store = new PostgresIdempotencyStore(jdbc, transactionManager, new ObjectMapper(), Duration.ofSeconds(2));
    }

    @AfterEach
    void cleanUp() {
        store.abandonDanglingReservationForTests();
    }

    @Test
    void overLongKeyComponentsReserveCompleteAndReplay() {
        EffectiveKey key = new EffectiveKey("/" + "r".repeat(4000), "h".repeat(300), "p".repeat(1000), "k".repeat(512));

        String token = store.reserve(key, "fp", TTL).fenceToken().orElseThrow();
        CachedResponse response = new CachedResponse(201, Map.of(), "{\"id\":1}".getBytes());
        store.complete(key, token, response, TTL);

        assertThat(store.find(key).orElseThrow().response()).isEqualTo(response);
        ReservationResult repeat = store.reserve(key, "fp", TTL);
        assertThat(repeat.outcome()).isEqualTo(ReservationResult.Outcome.EXISTS);
        assertThat(repeat.existing().orElseThrow().state()).isEqualTo(RecordState.COMPLETED);
    }

    @Test
    void overLongValuesSharingAPrefixStayDistinct() {
        String prefix = "k".repeat(300);
        EffectiveKey first = new EffectiveKey("/orders", "POST", "", prefix + "-1");
        EffectiveKey second = new EffectiveKey("/orders", "POST", "", prefix + "-2");

        String token = store.reserve(first, "fp", TTL).fenceToken().orElseThrow();
        store.complete(first, token, new CachedResponse(201, Map.of(), "{}".getBytes()), TTL);

        assertThat(store.find(second)).isEmpty();
    }

    @Test
    void valuesThatFitAreStoredVerbatim() {
        // Exactly at the column limits, counted in characters (code points) as
        // VARCHAR(n) does - including a supplementary character.
        String route = "/" + "r".repeat(509) + "😀";
        String key = "k".repeat(255);
        EffectiveKey effectiveKey = new EffectiveKey(route, "POST", "alice", key);

        String token = store.reserve(effectiveKey, "fp", TTL).fenceToken().orElseThrow();
        store.complete(effectiveKey, token, new CachedResponse(201, Map.of(), "{}".getBytes()), TTL);

        Map<String, Object> row = jdbc.queryForMap("SELECT route, handler, principal, idempotency_key FROM idempotency_record");
        assertThat(row).containsEntry("route", route)
                .containsEntry("handler", "POST")
                .containsEntry("principal", "alice")
                .containsEntry("idempotency_key", key);
    }
}
