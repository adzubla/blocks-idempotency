package io.adzubla.blocks.idempotency.validation;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.IdempotencyStoreQualifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * Fails application startup with a clear message if any {@code @Idempotent}
 * handler is misconfigured: {@link #validateKeyStrategy key strategy}
 * (exactly one of {@code header}/{@code fieldPath}), {@link #validateTtl
 * ttl} (a parseable {@code Duration} when set), or {@link #validateStore
 * store} (the resolved qualifier must have a matching {@link
 * IdempotencyStore} bean, resolved via {@link IdempotencyStoreQualifiers} -
 * the same resolution {@link
 * io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry} uses to
 * route each request to its store at runtime). Runs once, after every
 * singleton (including {@link RequestMappingHandlerMapping}) has finished
 * initializing, so the full set of mapped handlers is available to scan.
 */
public class IdempotentHandlerValidator implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(IdempotentHandlerValidator.class);

    private final ConfigurableListableBeanFactory beanFactory;
    private final RequestMappingHandlerMapping handlerMapping;
    private final IdempotencyProperties properties;

    public IdempotentHandlerValidator(ConfigurableListableBeanFactory beanFactory, RequestMappingHandlerMapping handlerMapping,
            IdempotencyProperties properties) {
        this.beanFactory = beanFactory;
        this.handlerMapping = handlerMapping;
        this.properties = properties;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Map<String, IdempotencyStore> storesByQualifier = IdempotencyStoreQualifiers.byQualifier(beanFactory);
        int validatedCount = 0;
        for (HandlerMethod handlerMethod : handlerMapping.getHandlerMethods().values()) {
            Method method = handlerMethod.getMethod();
            Idempotent annotation = method.getAnnotation(Idempotent.class);
            if (annotation == null) {
                continue;
            }
            validateKeyStrategy(method, annotation);
            validateTtl(method, annotation);
            validateStore(method, annotation, storesByQualifier);
            validatedCount++;
            log.info("Idempotent endpoint: {} strategy={}={} store={}", describe(method),
                    annotation.header().isEmpty() ? "fieldPath" : "header",
                    annotation.header().isEmpty() ? annotation.fieldPath() : annotation.header(),
                    annotation.store().isEmpty() ? properties.getDefaultStore() : annotation.store());
        }
        // Reached only if every @Idempotent handler above passed validation -
        // any failure throws IllegalStateException and aborts startup instead.
        log.info("Idempotency configuration valid: {} @Idempotent endpoint(s) validated", validatedCount);
    }

    private void validateKeyStrategy(Method method, Idempotent annotation) {
        boolean hasHeader = !annotation.header().isEmpty();
        boolean hasFieldPath = !annotation.fieldPath().isEmpty();
        if (hasHeader == hasFieldPath) {
            throw new IllegalStateException(describe(method) + ": exactly one of header()/fieldPath() must be set, found "
                    + (hasHeader ? "both" : "neither"));
        }
    }

    private void validateTtl(Method method, Idempotent annotation) {
        if (annotation.ttl().isEmpty()) {
            return;
        }
        try {
            DurationStyle.detectAndParse(annotation.ttl());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    describe(method) + ": ttl '" + annotation.ttl() + "' is not a valid Duration", e);
        }
    }

    private void validateStore(Method method, Idempotent annotation, Map<String, IdempotencyStore> storesByQualifier) {
        String qualifier = annotation.store().isEmpty() ? properties.getDefaultStore() : annotation.store();
        if (qualifier.isEmpty()) {
            throw new IllegalStateException(describe(method)
                    + ": no store configured - set store on @Idempotent or idempotency.default-store");
        }
        if (!storesByQualifier.containsKey(qualifier)) {
            throw new IllegalStateException(describe(method) + ": no " + IdempotencyStore.class.getSimpleName()
                    + " bean found for store qualifier '" + qualifier + "' - is the idempotency-store-" + qualifier
                    + " module on the classpath?");
        }
    }

    private static String describe(Method method) {
        return method.getDeclaringClass().getSimpleName() + "." + method.getName() + "()";
    }
}
