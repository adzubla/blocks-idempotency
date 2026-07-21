package io.adzubla.blocks.idempotency.web.config;

import io.adzubla.blocks.idempotency.config.IdempotencyAutoConfiguration;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.key.PrincipalClaimResolver;
import io.adzubla.blocks.idempotency.validation.IdempotentHandlerValidator;
import io.adzubla.blocks.idempotency.web.IdempotencyExceptionHandler;
import io.adzubla.blocks.idempotency.web.IdempotencyFilter;
import io.adzubla.blocks.idempotency.web.IdempotencyInterceptor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * HTTP/Servlet auto-configuration: registers the {@link IdempotentHandlerValidator}
 * (Slice 010 - fails startup fast on a misconfigured {@code @Idempotent}
 * handler, including one whose store qualifier resolves to no bean at all),
 * the {@link IdempotencyFilter}, {@link IdempotencyInterceptor}, and the
 * default {@link IdempotencyExceptionHandler} that translates the
 * interceptor's thrown {@code IdempotencyException}s into responses (an
 * application's own {@code @ControllerAdvice} can override any of them by
 * handling the same exception type).
 *
 * <p>{@code @AutoConfigureAfter(IdempotencyAutoConfiguration.class)} paired
 * with {@code @ConditionalOnBean(IdempotencyEngineRegistry.class)} is
 * load-bearing, mirroring the exact technique {@code
 * RedisIdempotencyStoreAutoConfiguration}/{@code
 * PostgresIdempotencyStoreAutoConfiguration} use via {@code
 * @AutoConfigureBefore(IdempotencyAutoConfiguration.class)}: without it, this
 * class's condition could be evaluated before core's {@code
 * IdempotencyEngineRegistry} bean exists, and the interceptor/filter would
 * silently never get registered - every {@code @Idempotent} endpoint would
 * run unprotected, with no error.
 */
@AutoConfiguration
@ConditionalOnWebApplication
@AutoConfigureAfter(IdempotencyAutoConfiguration.class)
@ConditionalOnBean(IdempotencyEngineRegistry.class)
public class IdempotencyWebAutoConfiguration {

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
