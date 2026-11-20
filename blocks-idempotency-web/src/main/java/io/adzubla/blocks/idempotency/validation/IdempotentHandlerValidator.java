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
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.core.ResolvableType;
import org.springframework.http.HttpEntity;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.context.request.async.WebAsyncTask;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;

/**
 * Fails application startup with a clear message if any {@code @Idempotent}
 * handler is misconfigured: {@link #validateKeyStrategy key strategy}
 * (exactly one of {@code header}/{@code fieldPath}), {@link #validateTtl
 * ttl} (a parseable {@code Duration} when set), or {@link #validateStore
 * store} (the resolved qualifier must have a matching {@link
 * IdempotencyStore} bean, resolved via {@link IdempotencyStoreQualifiers} -
 * the same resolution {@link
 * io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry} uses to
 * route each request to its store at runtime), or {@link #validateAsyncSupport
 * async support} (an asynchronous handler on a store whose reservation is
 * thread-bound, i.e. Postgres, is rejected). Runs once, after every
 * singleton (including {@link RequestMappingHandlerMapping}) has finished
 * initializing, so the full set of mapped handlers is available to scan.
 */
public class IdempotentHandlerValidator implements SmartInitializingSingleton {

    /** Return types Spring MVC completes on a different thread than the one that invoked the handler. */
    private static final List<Class<?>> ASYNC_RETURN_TYPES = List.of(Callable.class, WebAsyncTask.class,
            DeferredResult.class, CompletionStage.class, ResponseBodyEmitter.class, StreamingResponseBody.class);

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
            validateAsyncSupport(method, annotation, storesByQualifier);
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

    /**
     * Rejects an asynchronous handler on a store whose reservation is bound to the reserving
     * thread ({@link IdempotencyStore#supportsAsyncHandlers()} - Postgres): the handler would
     * finish on another thread, its result would never be committed, and the open reservation
     * would stay bound to a pooled servlet thread (issue 064).
     */
    private void validateAsyncSupport(Method method, Idempotent annotation, Map<String, IdempotencyStore> storesByQualifier) {
        String qualifier = annotation.store().isEmpty() ? properties.getDefaultStore() : annotation.store();
        IdempotencyStore store = storesByQualifier.get(qualifier);
        if (store.supportsAsyncHandlers() || !isAsyncReturnType(ResolvableType.forMethodReturnType(method))) {
            return;
        }
        throw new IllegalStateException(describe(method) + ": returns " + method.getGenericReturnType().getTypeName()
                + ", an async result, but store '" + qualifier + "' doesn't support async handlers - its reservation is "
                + "bound to the reserving thread. Make the handler synchronous, or use a store that supports async handlers");
    }

    private static boolean isAsyncReturnType(ResolvableType returnType) {
        Class<?> type = returnType.resolve(Object.class);
        if (HttpEntity.class.isAssignableFrom(type)) {
            // ResponseEntity<StreamingResponseBody>, ResponseEntity<DeferredResult<...>>, etc.
            return isAsyncReturnType(returnType.as(HttpEntity.class).getGeneric(0));
        }
        for (Class<?> asyncType : ASYNC_RETURN_TYPES) {
            if (asyncType.isAssignableFrom(type)) {
                return true;
            }
        }
        // Reactive types (Mono, Flux, ...) that Spring MVC adapts to an async result.
        return ReactiveAdapterRegistry.getSharedInstance().getAdapter(type) != null;
    }

    private static String describe(Method method) {
        return method.getDeclaringClass().getSimpleName() + "." + method.getName() + "()";
    }
}
