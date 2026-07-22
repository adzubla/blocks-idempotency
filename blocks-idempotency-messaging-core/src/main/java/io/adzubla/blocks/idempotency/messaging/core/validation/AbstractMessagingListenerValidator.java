package io.adzubla.blocks.idempotency.messaging.core.validation;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.IdempotencyStoreQualifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The broker-neutral startup-validation skeleton shared by every
 * message-listener broker: fails application startup with a clear message if
 * any {@code @Idempotent} listener method is misconfigured. The same {@code
 * header}/{@code fieldPath}/{@code ttl}/{@code store} checks {@code
 * IdempotentHandlerValidator} enforces for HTTP, plus the WAIT-rejection rule
 * from {@code docs/adr/0005-messaging-wait-disabled.md}, all live here (Slice
 * 048, extracted from the Kafka module's Slice 040 validator and the RabbitMQ
 * module's Slice 045 validator, which had become two copies of the same
 * checks).
 *
 * <p>The genuinely broker-specific pieces are left to subclass seams: which
 * listener annotation marks a method ({@link #isListenerMethod}), the
 * required message-parameter check ({@link #validateSignature} - the advice
 * resolves the key and fingerprint from a raw {@code ConsumerRecord}/{@code
 * Message}, so a POJO/{@code @Payload}-only listener would otherwise fail only
 * at the first delivery), and the WAIT-rejection message wording ({@link
 * #waitNotSupportedMessage}), which cites each broker's own reason for the ADR
 * 0005 restriction.
 *
 * <p>Neither broker enumerates methods via a listener-endpoint registry: once
 * a listener method is registered, the registry exposes only the resulting
 * {@code MessageListenerContainer}s, not the original bean/{@link Method}/
 * annotations that produced them. Instead, every registered bean's
 * (proxy-unwrapped via {@link ClassUtils#getUserClass}) declared methods are
 * scanned directly for the {@code @Idempotent} + listener-annotation pair -
 * the same annotation-driven discovery the broker's own {@code
 * ListenerAnnotationBeanPostProcessor} performs internally. Every registered
 * bean definition is scanned by <em>type</em> (singleton or not - a
 * prototype-scoped listener bean is still processed once per instantiation,
 * so it needs the same startup check), never by eagerly instantiating one
 * just to inspect it ({@code allowFactoryBeanInit=false} throughout). Runs
 * once, after every singleton has finished initializing (by which point
 * listener processing - a {@code BeanPostProcessor} callback that runs
 * per-bean, during creation - has already happened for all of them).
 */
public abstract class AbstractMessagingListenerValidator implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(AbstractMessagingListenerValidator.class);

    protected final ConfigurableListableBeanFactory beanFactory;
    protected final IdempotencyProperties properties;

    protected AbstractMessagingListenerValidator(ConfigurableListableBeanFactory beanFactory, IdempotencyProperties properties) {
        this.beanFactory = beanFactory;
        this.properties = properties;
    }

    /** Broker seam: does {@code method} carry this broker's listener annotation (e.g. {@code @KafkaListener})? */
    protected abstract boolean isListenerMethod(Method method);

    /** Broker seam: fail if {@code method} doesn't accept this broker's raw message-parameter type. */
    protected abstract void validateSignature(Method method);

    /** Broker seam: the WAIT-rejection message, citing this broker's own reason for the ADR 0005 restriction. */
    protected abstract String waitNotSupportedMessage(Method method);

    /** Broker seam: broker name for the summary log line (e.g. {@code "Kafka"}). */
    protected abstract String brokerName();

    @Override
    public void afterSingletonsInstantiated() {
        Map<String, IdempotencyStore> storesByQualifier = IdempotencyStoreQualifiers.byQualifier(beanFactory);
        AtomicInteger validatedCount = new AtomicInteger();
        for (String beanName : beanFactory.getBeanNamesForType(Object.class, true, false)) {
            Class<?> beanType = beanFactory.getType(beanName, false);
            if (beanType == null) {
                continue;
            }
            Class<?> targetType = ClassUtils.getUserClass(beanType);
            ReflectionUtils.doWithMethods(targetType, method -> validateIfIdempotentListener(method, storesByQualifier, validatedCount),
                    ReflectionUtils.USER_DECLARED_METHODS);
        }
        // Reached only if every @Idempotent listener method above passed validation -
        // any failure throws IllegalStateException and aborts startup instead.
        log.info("Idempotency {} configuration valid: {} @Idempotent listener method(s) validated", brokerName(), validatedCount.get());
    }

    private void validateIfIdempotentListener(Method method, Map<String, IdempotencyStore> storesByQualifier, AtomicInteger validatedCount) {
        Idempotent annotation = method.getAnnotation(Idempotent.class);
        if (annotation == null || !isListenerMethod(method)) {
            return;
        }
        validateWhenInProgress(method, annotation);
        validateSignature(method);
        validateKeyStrategy(method, annotation);
        validateTtl(method, annotation);
        validateStore(method, annotation, storesByQualifier);
        validatedCount.incrementAndGet();
        log.info("Idempotent {} listener: {} strategy={}={} store={}", brokerName(), describe(method),
                annotation.header().isEmpty() ? "fieldPath" : "header",
                annotation.header().isEmpty() ? annotation.fieldPath() : annotation.header(),
                annotation.store().isEmpty() ? properties.getDefaultStore() : annotation.store());
    }

    private void validateWhenInProgress(Method method, Idempotent annotation) {
        WhenInProgress whenInProgress = annotation.whenInProgress();
        if (whenInProgress == WhenInProgress.DEFAULT) {
            whenInProgress = properties.getDefaultWhenInProgress();
            if (whenInProgress == WhenInProgress.DEFAULT) {
                // Guards the same "never a sentinel" contract PolicyResolver.requireResolved
                // enforces for the runtime path - a misconfigured global default (YAML
                // relaxed-binding happily accepts DEFAULT as an enum value) must fail here,
                // at startup, not resolve silently through to a request/delivery at runtime.
                throw new IllegalStateException("idempotency.default-when-in-progress must not be DEFAULT");
            }
        }
        if (whenInProgress == WhenInProgress.WAIT) {
            throw new IllegalStateException(waitNotSupportedMessage(method));
        }
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
            throw new IllegalStateException(describe(method) + ": ttl '" + annotation.ttl() + "' is not a valid Duration", e);
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

    protected static String describe(Method method) {
        return method.getDeclaringClass().getSimpleName() + "." + method.getName() + "()";
    }
}
