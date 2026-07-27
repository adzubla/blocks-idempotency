package io.adzubla.blocks.idempotency.store.postgres;

import io.adzubla.blocks.idempotency.config.IdempotencyAutoConfiguration;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.web.IdempotencyInterceptor;
import io.adzubla.blocks.idempotency.web.config.IdempotencyWebAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Regression test: {@link IdempotencyAutoConfiguration} is gated on {@code
 * @ConditionalOnBean(IdempotencyStore.class)}, and Spring Boot evaluates
 * auto-configurations in alphabetical class-name order unless told
 * otherwise - {@code io.adzubla.blocks.idempotency.config} sorts before
 * {@code io.adzubla.blocks.idempotency.store.postgres}, so without an
 * explicit ordering hint the engine's condition was evaluated before this
 * store's bean existed. The engine/interceptor then silently never got
 * registered: every {@code @Idempotent} endpoint ran completely
 * unprotected, with no startup error to flag it (caught live in the Slice
 * 017 end-to-end suite, {@link io.adzubla.blocks.idempotency.web.PostgresIdempotencyEndToEndTest}).
 *
 * <p>{@link PostgresIdempotencyStoreAutoConfiguration}'s {@code
 * @AutoConfigureBefore(IdempotencyAutoConfiguration.class)}, and (since ADR
 * 0006 split HTTP integration into {@code blocks-idempotency-web}) {@link
 * IdempotencyWebAutoConfiguration}'s {@code
 * @AutoConfigureAfter(IdempotencyAutoConfiguration.class)} +
 * {@code @ConditionalOnBean(IdempotencyEngineRegistry.class)}, fix the
 * ordering outright, so this asserts the engine and interceptor beans exist
 * even though all three auto-configurations are deliberately listed here in
 * the bug-triggering order (web and the engine config both before what they
 * each depend on).
 */
class PostgresIdempotencyStoreAutoConfigurationOrderingTest {

    private final WebApplicationContextRunner runner = new WebApplicationContextRunner()
            .withUserConfiguration(RequiredBeans.class)
            .withConfiguration(AutoConfigurations.of(
                    IdempotencyWebAutoConfiguration.class,
                    IdempotencyAutoConfiguration.class,
                    PostgresIdempotencyStoreAutoConfiguration.class));

    @Test
    void theEngineAndInterceptorAreRegisteredEvenThoughTheStoreAutoConfigurationIsListedSecond() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(PostgresIdempotencyStore.class);
            assertThat(context).hasSingleBean(IdempotencyEngineRegistry.class);
            assertThat(context).hasSingleBean(IdempotencyInterceptor.class);
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

        @Bean
        RequestMappingHandlerMapping requestMappingHandlerMapping() {
            return new RequestMappingHandlerMapping();
        }
    }
}
