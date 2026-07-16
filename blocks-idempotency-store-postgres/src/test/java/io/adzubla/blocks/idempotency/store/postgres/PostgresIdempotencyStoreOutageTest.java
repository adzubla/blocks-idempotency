package io.adzubla.blocks.idempotency.store.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import io.adzubla.blocks.idempotency.store.StoreUnavailableException;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves a genuine Postgres outage (a stopped container, not a mock) surfaces
 * as {@link StoreUnavailableException} from every {@link PostgresIdempotencyStore}
 * operation, exercising the same {@code onStoreFailure} posture Redis and the
 * in-memory store already do (PRD: "store-failure posture uniformly ... across
 * both stores").
 *
 * <p>A fresh, per-test container (not the class-shared style used elsewhere in
 * this module) - each test stops its own connection mid-flight, and {@code
 * complete}/{@code release} each need their own cleanly-established reservation
 * beforehand, so sharing one container/stop across methods isn't workable.
 */
@Testcontainers
class PostgresIdempotencyStoreOutageTest {

    @Container
    private final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

    @Test
    void reserveAndFindSurfaceStoreUnavailableAfterTheContainerStops() {
        Fixture fixture = newFixture();

        // Sanity check: the store genuinely works before the container stops
        // - a false positive here would make the exceptions below meaningless.
        ReservationResult sanity = fixture.store.reserve(key("sanity-1"), "fp", Duration.ofSeconds(30));
        assertThat(sanity.outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
        fixture.store.release(key("sanity-1"), sanity.fenceToken().orElseThrow());

        postgres.stop();

        assertThatThrownBy(() -> fixture.store.reserve(key("outage-1"), "fp", Duration.ofSeconds(30)))
                .isInstanceOf(StoreUnavailableException.class);
        assertThatThrownBy(() -> fixture.store.find(key("outage-1")))
                .isInstanceOf(StoreUnavailableException.class);

        fixture.dataSource.close();
    }

    @Test
    void completeSurfacesStoreUnavailableWhenTheConnectionDiesAfterReserve() {
        Fixture fixture = newFixture();
        EffectiveKey key = key("complete-outage-1");
        String fenceToken = fixture.store.reserve(key, "fp", Duration.ofSeconds(30)).fenceToken().orElseThrow();

        postgres.stop();

        assertThatThrownBy(() -> fixture.store.complete(
                key, fenceToken, new CachedResponse(201, Map.of(), "{}".getBytes()), Duration.ofSeconds(30)))
                .isInstanceOf(StoreUnavailableException.class);

        fixture.dataSource.close();
    }

    @Test
    void releaseSurfacesStoreUnavailableWhenTheConnectionDiesAfterReserve() {
        Fixture fixture = newFixture();
        EffectiveKey key = key("release-outage-1");
        String fenceToken = fixture.store.reserve(key, "fp", Duration.ofSeconds(30)).fenceToken().orElseThrow();

        postgres.stop();

        assertThatThrownBy(() -> fixture.store.release(key, fenceToken))
                .isInstanceOf(StoreUnavailableException.class);

        fixture.dataSource.close();
    }

    @Test
    void aGenuineDataErrorIsNotMisclassifiedAsStoreUnavailable() {
        Fixture fixture = newFixture();
        EffectiveKey key = key("bad-status-1");
        String fenceToken = fixture.store.reserve(key, "fp", Duration.ofSeconds(30)).fenceToken().orElseThrow();

        // response_status is a SMALLINT column (max 32767, migration
        // V1__idempotency_record.sql) - deliberately out of range to trigger
        // a genuine Postgres data error (not a connection failure), proving
        // asStoreUnavailableIfConnectionFailure() doesn't over-broadly treat
        // every failure as an outage and silently disable idempotency
        // protection for what's actually a caller bug.
        CachedResponse outOfRangeStatus = new CachedResponse(999_999, Map.of(), "{}".getBytes());

        assertThatThrownBy(() -> fixture.store.complete(key, fenceToken, outOfRangeStatus, Duration.ofSeconds(30)))
                .isNotInstanceOf(StoreUnavailableException.class);

        fixture.dataSource.close();
    }

    private Fixture newFixture() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(postgres.getJdbcUrl());
        dataSource.setUsername(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        dataSource.setConnectionTimeout(2000);
        dataSource.setValidationTimeout(2000);
        dataSource.setMaximumPoolSize(4);
        Flyway.configure().dataSource(dataSource).load().migrate();

        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        PostgresIdempotencyStore store = new PostgresIdempotencyStore(
                new JdbcTemplate(dataSource), transactionManager, new ObjectMapper(), Duration.ofSeconds(2));
        return new Fixture(dataSource, store);
    }

    private static EffectiveKey key(String value) {
        return new EffectiveKey("POST", "/orders", "", value);
    }

    private record Fixture(HikariDataSource dataSource, PostgresIdempotencyStore store) {
    }
}
