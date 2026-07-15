package io.adzubla.blocks.idempotency.store.postgres;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Proves {@link PostgresIdempotencyStoreAutoConfiguration} actually wires
 * {@link PostgresIdempotencyCleanupJob} into a real Spring context (Slice
 * 018's "a scheduled cleanup job deletes rows past {@code expires_at}" - the
 * job class itself is exercised directly against Postgres in {@link
 * PostgresIdempotencyCleanupJobTest}; this instead checks the wiring: that it
 * shows up by default, and that {@code idempotency.postgres.cleanup.enabled=false}
 * genuinely turns it off).
 */
class PostgresIdempotencyCleanupJobAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(RequiredBeans.class)
            .withConfiguration(AutoConfigurations.of(PostgresIdempotencyStoreAutoConfiguration.class));

    @Test
    void theCleanupJobIsRegisteredByDefault() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(PostgresIdempotencyCleanupJob.class);
        });
    }

    @Test
    void theCleanupJobIsAbsentWhenExplicitlyDisabled() {
        runner.withPropertyValues("idempotency.postgres.cleanup.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(PostgresIdempotencyCleanupJob.class);
            // The store itself is unaffected by the cleanup toggle.
            assertThat(context).hasSingleBean(PostgresIdempotencyStore.class);
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class RequiredBeans {

        @Bean
        DataSource dataSource() {
            return mock(DataSource.class);
        }

        @Bean
        JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        @Bean
        PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new JdbcTransactionManager(dataSource);
        }

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
    }
}
