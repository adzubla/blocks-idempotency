package io.adzubla.blocks.idempotency.config;

import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.key.DefaultPrincipalClaimResolver;
import io.adzubla.blocks.idempotency.key.PrincipalClaimResolver;
import io.adzubla.blocks.idempotency.metrics.IdempotencyMetrics;
import io.adzubla.blocks.idempotency.metrics.MicrometerIdempotencyMetrics;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.IdempotencyStoreQualifiers;
import io.adzubla.blocks.idempotency.validation.IdempotentHandlerValidator;
import io.adzubla.blocks.idempotency.web.IdempotencyExceptionHandler;
import io.adzubla.blocks.idempotency.web.IdempotencyFilter;
import io.adzubla.blocks.idempotency.web.IdempotencyInterceptor;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * Core auto-configuration: binds {@link IdempotencyProperties}, registers the
 * {@link IdempotentHandlerValidator} (Slice 010 - fails startup fast on a
 * misconfigured {@code @Idempotent} handler, including one whose store
 * qualifier resolves to no bean at all), and, once at least one {@link
 * IdempotencyStore} bean is present (provided by one or more store modules,
 * or by the application itself), wires the {@link IdempotencyFilter}, an
 * {@link IdempotencyEngineRegistry} (one engine per store qualifier),
 * {@link IdempotencyInterceptor}, and the default {@link
 * IdempotencyExceptionHandler} that translates the interceptor's thrown
 * {@code IdempotencyException}s into responses (an application's own {@code
 * @ControllerAdvice} can override any of them by handling the same exception
 * type). It deliberately does not provide a default {@code IdempotencyStore}
 * - a store is chosen per endpoint by qualifier and provided by a store
 * module; multiple store modules (e.g. Redis and Postgres) may be on the
 * classpath at once, each endpoint routed to its own by {@code
 * IdempotencyEngineRegistry}.
 *
 * <p>{@link IdempotencyMetrics} (Slice 011) resolves to {@link
 * MicrometerIdempotencyMetrics} when {@code idempotency.metrics.enabled}
 * (default {@code true}) and a {@link MeterRegistry} bean are both present,
 * otherwise to {@link NoOpIdempotencyMetrics}.
 */
@AutoConfiguration
@ConditionalOnWebApplication
@EnableConfigurationProperties(IdempotencyProperties.class)
public class IdempotencyAutoConfiguration {

    @Bean
    public FilterRegistrationBean<IdempotencyFilter> idempotencyFilterRegistration() {
        FilterRegistrationBean<IdempotencyFilter> registration = new FilterRegistrationBean<>(new IdempotencyFilter());
        registration.addUrlPatterns("/*");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }

    @Bean
    public IdempotentHandlerValidator idempotentHandlerValidator(ConfigurableListableBeanFactory beanFactory,
            RequestMappingHandlerMapping requestMappingHandlerMapping, IdempotencyProperties properties) {
        return new IdempotentHandlerValidator(beanFactory, requestMappingHandlerMapping, properties);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(IdempotencyStore.class)
    static class EngineConfiguration {

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
        public IdempotencyEngineRegistry idempotencyEngineRegistry(ConfigurableListableBeanFactory beanFactory, IdempotencyMetrics metrics) {
            return new IdempotencyEngineRegistry(IdempotencyStoreQualifiers.byQualifier(beanFactory), metrics);
        }

        @Bean
        @ConditionalOnMissingBean
        public PrincipalClaimResolver principalClaimResolver() {
            return new DefaultPrincipalClaimResolver();
        }

        @Bean
        public IdempotencyInterceptor idempotencyInterceptor(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties,
                PrincipalClaimResolver principalClaimResolver) {
            return new IdempotencyInterceptor(engineRegistry, properties, principalClaimResolver);
        }

        @Bean
        @ConditionalOnMissingBean(IdempotencyExceptionHandler.class)
        public IdempotencyExceptionHandler idempotencyExceptionHandler() {
            return new IdempotencyExceptionHandler();
        }

        @Bean
        public WebMvcConfigurer idempotencyWebMvcConfigurer(IdempotencyInterceptor interceptor) {
            return new WebMvcConfigurer() {
                @Override
                public void addInterceptors(InterceptorRegistry registry) {
                    registry.addInterceptor(interceptor);
                }
            };
        }
    }
}
