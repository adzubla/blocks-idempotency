package io.adzubla.blocks.idempotency.store.postgres;

import org.flywaydb.core.Flyway;

import javax.sql.DataSource;

/**
 * Applies the schema the way an application would: the library's shipped script (copied by the
 * application into its own migrations) plus the test-only {@code test_orders} table. The shipped
 * script is not on Flyway's default location, so the tests point Flyway at it explicitly.
 */
final class PostgresTestSchema {

    /** Where the library ships its script (not scanned by default), and the test-only migration. */
    static final String LOCATIONS = "classpath:META-INF/blocks-idempotency/postgres,classpath:db/migration";

    private PostgresTestSchema() {
    }

    static void migrate(DataSource dataSource) {
        Flyway.configure().dataSource(dataSource).locations(LOCATIONS.split(",")).load().migrate();
    }
}
