package io.adzubla.blocks.idempotency.store;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resolves every {@link IdempotencyStore} bean in the context to the qualifier(s)
 * a {@code @Idempotent(store = ...)}/{@code idempotency.default-store} value can
 * address it by: its bean name, and, if present, the {@code @Qualifier} on its
 * {@code @Bean} factory method (the convention every store module uses, e.g.
 * {@code @Bean @Qualifier("redis")} - see {@link
 * io.adzubla.blocks.idempotency.validation.IdempotentHandlerValidator}, the
 * original home of this resolution logic, and {@link
 * io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry}, which uses it
 * to route each request to the store its endpoint asked for.
 */
public final class IdempotencyStoreQualifiers {

    private IdempotencyStoreQualifiers() {
    }

    public static Map<String, IdempotencyStore> byQualifier(ConfigurableListableBeanFactory beanFactory) {
        Map<String, IdempotencyStore> stores = new LinkedHashMap<>();
        for (String beanName : beanFactory.getBeanNamesForType(IdempotencyStore.class)) {
            IdempotencyStore store = beanFactory.getBean(beanName, IdempotencyStore.class);
            stores.put(beanName, store);
            Qualifier qualifierAnnotation = beanFactory.findAnnotationOnBean(beanName, Qualifier.class);
            if (qualifierAnnotation != null) {
                stores.put(qualifierAnnotation.value(), store);
            }
        }
        return stores;
    }
}
