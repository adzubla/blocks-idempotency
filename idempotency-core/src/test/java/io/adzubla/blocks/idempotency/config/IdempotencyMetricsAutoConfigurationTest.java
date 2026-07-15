package io.adzubla.blocks.idempotency.config;

import io.adzubla.blocks.idempotency.metrics.IdempotencyMetrics;
import io.adzubla.blocks.idempotency.metrics.MicrometerIdempotencyMetrics;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import io.adzubla.blocks.idempotency.validation.IdempotentHandlerValidator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves Slice 011's "metrics can be disabled via configuration" acceptance
 * criterion at the wiring level: no {@code @Idempotent} handler is registered
 * in these contexts, so {@link IdempotentHandlerValidator}
 * has nothing to validate and the store bean's qualifier doesn't matter here.
 */
class IdempotencyMetricsAutoConfigurationTest {

    private final WebApplicationContextRunner runner = new WebApplicationContextRunner()
            .withUserConfiguration(StoreConfig.class);

    @Test
    void metricsEnabledByDefaultWithAMeterRegistryUsesMicrometer() {
        runner.withUserConfiguration(MeterRegistryConfig.class).run(context ->
                assertThat(context.getBean(IdempotencyMetrics.class)).isInstanceOf(MicrometerIdempotencyMetrics.class));
    }

    @Test
    void metricsDisabledFallsBackToNoOpEvenWithAMeterRegistryPresent() {
        runner.withUserConfiguration(MeterRegistryConfig.class)
                .withPropertyValues("idempotency.metrics.enabled=false")
                .run(context -> assertThat(context.getBean(IdempotencyMetrics.class)).isInstanceOf(NoOpIdempotencyMetrics.class));
    }

    @Test
    void noMeterRegistryFallsBackToNoOpWithoutFailingStartup() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(IdempotencyMetrics.class)).isInstanceOf(NoOpIdempotencyMetrics.class);
        });
    }

    @Test
    void userSuppliedMetricsBeanWinsOverMicrometerEvenWithAMeterRegistryPresent() {
        // A MeterRegistry commonly coexists with a user's own IdempotencyMetrics
        // (e.g. an app wiring its own bean alongside Actuator) - without
        // @ConditionalOnMissingBean on the Micrometer bean, this combination
        // would previously register both and fail startup with
        // NoUniqueBeanDefinitionException instead of letting the user's bean win.
        runner.withUserConfiguration(MeterRegistryConfig.class, CustomMetricsConfig.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(IdempotencyMetrics.class)).isInstanceOf(CustomIdempotencyMetrics.class);
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class StoreConfig {
        @Bean
        IdempotencyStore idempotencyStore() {
            return new InMemoryIdempotencyStore();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MeterRegistryConfig {
        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomMetricsConfig {
        @Bean
        IdempotencyMetrics customIdempotencyMetrics() {
            return new CustomIdempotencyMetrics();
        }
    }

    /** A distinct implementation - neither {@link MicrometerIdempotencyMetrics} nor {@link NoOpIdempotencyMetrics} - to prove identity, not just fallback behavior. */
    static class CustomIdempotencyMetrics implements IdempotencyMetrics {
        @Override
        public void recordReplay() {
        }

        @Override
        public void recordCollision() {
        }

        @Override
        public void recordConcurrency() {
        }

        @Override
        public void recordFailOpen() {
        }
    }
}
