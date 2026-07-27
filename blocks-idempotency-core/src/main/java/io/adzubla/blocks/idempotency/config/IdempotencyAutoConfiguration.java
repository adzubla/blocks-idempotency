package io.adzubla.blocks.idempotency.config;

import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.key.DefaultPrincipalClaimResolver;
import io.adzubla.blocks.idempotency.key.PrincipalClaimResolver;
import io.adzubla.blocks.idempotency.metrics.IdempotencyMetrics;
import io.adzubla.blocks.idempotency.metrics.MicrometerIdempotencyMetrics;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.IdempotencyStoreQualifiers;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Transport-neutral auto-configuration: binds {@link IdempotencyProperties}
 * and, once at least one {@link IdempotencyStore} bean is present (provided
 * by one or more store modules, or by the application itself), wires an
 * {@link IdempotencyEngineRegistry} (one engine per store qualifier) and the
 * default {@link PrincipalClaimResolver}. It deliberately does not provide a
 * default {@code IdempotencyStore} - a store is chosen per endpoint by
 * qualifier and provided by a store module; multiple store modules (e.g.
 * Redis and Postgres) may be on the classpath at once, each endpoint routed
 * to its own by {@code IdempotencyEngineRegistry}.
 *
 * <p>Not conditional on a web application: this bean set (engine, metrics,
 * principal resolver) is used by every transport adapter (HTTP via {@code
 * blocks-idempotency-web}, and future messaging modules), not just servlet
 * apps. The class keeps its name and package so
 * {@code RedisIdempotencyStoreAutoConfiguration}/{@code
 * PostgresIdempotencyStoreAutoConfiguration}'s load-bearing {@code
 * @AutoConfigureBefore(IdempotencyAutoConfiguration.class)} ordering, and this
 * module's own {@code AutoConfiguration.imports}, stay valid untouched. A
 * transport adapter's own auto-configuration (e.g. {@code
 * IdempotencyWebAutoConfiguration} in {@code blocks-idempotency-web}) depends
 * on the {@link IdempotencyEngineRegistry} bean registered here via the same
 * {@code @AutoConfigureAfter}/{@code @ConditionalOnBean} technique.
 *
 * <p>{@link IdempotencyMetrics} (Slice 011) resolves to {@link
 * MicrometerIdempotencyMetrics} when {@code idempotency.metrics.enabled}
 * (default {@code true}) and a {@link MeterRegistry} bean are both present,
 * otherwise to {@link NoOpIdempotencyMetrics}.
 */
@AutoConfiguration
@ConditionalOnBean(IdempotencyStore.class)
@EnableConfigurationProperties(IdempotencyProperties.class)
public class IdempotencyAutoConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = "idempotency.metrics", name = "enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(IdempotencyMetrics.class)
    public IdempotencyMetrics micrometerIdempotencyMetrics(MeterRegistry registry) {
        return new MicrometerIdempotencyMetrics(registry);
    }

    @Bean
    @ConditionalOnMissingBean(IdempotencyMetrics.class)
    public IdempotencyMetrics noOpIdempotencyMetrics() {
        return NoOpIdempotencyMetrics.INSTANCE;
    }

    @Bean
    public IdempotencyEngineRegistry idempotencyEngineRegistry(ConfigurableListableBeanFactory beanFactory, IdempotencyProperties properties,
            IdempotencyMetrics metrics) {
        return new IdempotencyEngineRegistry(IdempotencyStoreQualifiers.byQualifier(beanFactory), properties.getPollInterval(),
                properties.getPollJitter(), metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    public PrincipalClaimResolver principalClaimResolver() {
        return new DefaultPrincipalClaimResolver();
    }
}
