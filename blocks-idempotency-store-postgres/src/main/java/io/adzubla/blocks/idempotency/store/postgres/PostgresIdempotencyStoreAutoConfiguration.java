package io.adzubla.blocks.idempotency.store.postgres;

import io.adzubla.blocks.idempotency.config.IdempotencyAutoConfiguration;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.ObjectMapper;

/**
 * Registers the Postgres-backed store under the {@code "postgres"} qualifier when
 * spring-jdbc is on the classpath.
 *
 * <p>{@code @AutoConfigureBefore(IdempotencyAutoConfiguration.class)} is load-bearing:
 * that class's {@code EngineConfiguration} is gated on {@code @ConditionalOnBean(
 * IdempotencyStore.class)}, and without an explicit ordering the two auto-configurations
 * race on classpath/JAR scan order - if this one loses, the store bean doesn't exist yet
 * when the condition is evaluated, and the engine/interceptor silently never get
 * registered (every {@code @Idempotent} endpoint runs unprotected, with no error).
 *
 * <p>Also registers {@link PostgresStoreProperties} ({@code idempotency.postgres.*}
 * - {@code lock-timeout}, {@code cleanup.*}) and, unless {@code
 * idempotency.postgres.cleanup.enabled=false}, the {@link
 * PostgresIdempotencyCleanupJob} bean that sweeps expired rows (Slice 018 -
 * Postgres has no native TTL). The job manages its own schedule rather than
 * relying on {@code @Scheduled}/{@code TaskScheduler}, so it needs nothing
 * further wired here.
 */
@AutoConfiguration
@AutoConfigureBefore(IdempotencyAutoConfiguration.class)
@ConditionalOnClass(JdbcTemplate.class)
@EnableConfigurationProperties(PostgresStoreProperties.class)
public class PostgresIdempotencyStoreAutoConfiguration {

    @Bean
    @Qualifier(PostgresIdempotencyStore.QUALIFIER)
    @ConditionalOnMissingBean(name = "postgresIdempotencyStore")
    public IdempotencyStore postgresIdempotencyStore(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager,
            ObjectMapper objectMapper, PostgresStoreProperties properties) {
        return new PostgresIdempotencyStore(jdbcTemplate, transactionManager, objectMapper, properties.getLockTimeout());
    }

    @Bean
    @ConditionalOnMissingBean(PostgresIdempotencyCleanupJob.class)
    @ConditionalOnProperty(prefix = "idempotency.postgres.cleanup", name = "enabled", havingValue = "true", matchIfMissing = true)
    public PostgresIdempotencyCleanupJob postgresIdempotencyCleanupJob(JdbcTemplate jdbcTemplate, PostgresStoreProperties properties) {
        return new PostgresIdempotencyCleanupJob(jdbcTemplate, properties.getCleanup().getInterval());
    }
}
