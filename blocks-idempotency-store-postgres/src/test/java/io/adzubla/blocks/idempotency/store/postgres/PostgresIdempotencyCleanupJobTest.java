package io.adzubla.blocks.idempotency.store.postgres;

import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@link PostgresIdempotencyCleanupJob} against a real Postgres (Slice 018):
 * proves the sweep deletes rows past {@code expires_at} and leaves everything
 * else untouched. Invokes {@link PostgresIdempotencyCleanupJob#deleteExpiredRecords()}
 * directly rather than waiting on its internal schedule.
 */
@Testcontainers
class PostgresIdempotencyCleanupJobTest {

    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbc;

    private PostgresIdempotencyCleanupJob job;

    @BeforeAll
    static void migrate() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        jdbc = new JdbcTemplate(dataSource);

        Flyway.configure().dataSource(dataSource).load().migrate();
    }

    @AfterAll
    static void closeDataSource() {
        dataSource.close();
    }

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM idempotency_record");
        // A long interval - the test drives the sweep directly, never waiting on the schedule.
        job = new PostgresIdempotencyCleanupJob(jdbc, Duration.ofMinutes(30));
    }

    @AfterEach
    void tearDown() {
        job.destroy();
    }

    @Test
    void deletesRowsPastExpiresAtAndLeavesLiveRowsAlone() {
        insertRecord("expired-1", "-1 hour");
        insertRecord("expired-2", "-1 second");
        insertRecord("live-1", "+1 hour");

        int deleted = job.deleteExpiredRecords();

        assertThat(deleted).isEqualTo(2);
        assertThat(remainingKeys()).containsExactly("live-1");
    }

    @Test
    void isANoOpWhenNothingHasExpired() {
        insertRecord("live-1", "+1 hour");

        int deleted = job.deleteExpiredRecords();

        assertThat(deleted).isZero();
        assertThat(remainingKeys()).containsExactly("live-1");
    }

    @Test
    void theScheduleItselfSweepsExpiredRowsWithoutAnyoneCallingDeleteExpiredRecords() {
        job.destroy(); // replace the 30-minute-interval job from setUp with a fast one
        job = new PostgresIdempotencyCleanupJob(jdbc, Duration.ofMillis(50));
        insertRecord("expired-1", "-1 hour");

        job.afterPropertiesSet();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(remainingKeys()).isEmpty());
    }

    private static void insertRecord(String key, String expiresOffset) {
        jdbc.update("""
                INSERT INTO idempotency_record
                    (route, handler, principal, idempotency_key, fingerprint, reservation_token, created_at, expires_at)
                VALUES ('/orders', 'POST', '', ?, 'fp', 'token-' || ?, clock_timestamp(), clock_timestamp() + ?::interval)
                """, key, key, expiresOffset);
    }

    private static List<String> remainingKeys() {
        return jdbc.queryForList("SELECT idempotency_key FROM idempotency_record ORDER BY idempotency_key", String.class);
    }
}
