package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import io.adzubla.blocks.idempotency.validation.IdempotentHandlerValidator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spring Boot Actuator registers a second {@link RequestMappingHandlerMapping}
 * bean ({@code controllerEndpointHandlerMapping}); the library must still
 * start, binding its validator to Spring MVC's own mapping.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = MultipleHandlerMappingsStartupTest.TestApplication.class,
        properties = "idempotency.default-store=" + InMemoryIdempotencyStore.QUALIFIER)
class MultipleHandlerMappingsStartupTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void startsWithASecondRequestMappingHandlerMappingBeanPresent() {
        assertThat(context.getBeansOfType(RequestMappingHandlerMapping.class)).hasSizeGreaterThan(1);
        assertThat(context.getBean(IdempotentHandlerValidator.class)).isNotNull();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean(InMemoryIdempotencyStore.QUALIFIER)
        IdempotencyStore idempotencyStore() {
            return new InMemoryIdempotencyStore();
        }

        /** Same shape as Actuator's {@code controllerEndpointHandlerMapping}. */
        @Bean
        RequestMappingHandlerMapping additionalHandlerMapping() {
            return new RequestMappingHandlerMapping();
        }
    }
}
