package io.adzubla.blocks.idempotency.store.postgres;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The schema script must not sit on Flyway's default {@code classpath:db/migration} location: Spring
 * Boot would run it in every application that has this jar, and a library-owned "V1" collides with the
 * application's own migrations ("Found more than one migration with version 1"). It ships under
 * {@code META-INF} for the application to copy into its own migrations instead.
 */
class PostgresSchemaScriptShippingTest {

    @Test
    void theSchemaScriptIsShippedButNotOnFlywaysDefaultLocation() {
        ClassLoader loader = getClass().getClassLoader();

        assertThat(loader.getResource("META-INF/blocks-idempotency/postgres/V1__idempotency_record.sql")).isNotNull();
        assertThat(loader.getResource("db/migration/V1__idempotency_record.sql")).isNull();
    }
}
