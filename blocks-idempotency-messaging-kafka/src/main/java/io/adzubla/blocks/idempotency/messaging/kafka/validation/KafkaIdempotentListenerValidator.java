package io.adzubla.blocks.idempotency.messaging.kafka.validation;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.IdempotencyStoreQualifiers;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fails application startup with a clear message if any {@code @Idempotent}
 * {@code @KafkaListener} method is misconfigured: the same {@code
 * header}/{@code fieldPath}/{@code ttl}/{@code store} checks {@code
 * IdempotentHandlerValidator} enforces for HTTP (no shared helper - the
 * checks are re-implemented directly here, mirroring rather than reusing
 * that class, since it's wired to {@code RequestMappingHandlerMapping} and
 * lives in {@code blocks-idempotency-web}), plus {@link #validateSignature a
 * check that the method accepts a {@link
 * org.apache.kafka.clients.consumer.ConsumerRecord} parameter} (the advice
 * resolves the key and fingerprint from it, so a POJO/{@code @Payload}-only
 * listener would otherwise fail only at the first delivery), plus one
 * Kafka-specific rule from {@code docs/adr/0005-messaging-wait-disabled.md}: {@link
 * #validateWhenInProgress whenInProgress=WAIT} is rejected outright - only
 * {@code REJECT} is supported for v1, since blocking a listener container
 * thread in {@code store.await()} risks missing {@code
 * max.poll.interval.ms} and triggering a partition rebalance.
 *
 * <p>Unlike the HTTP validator, this doesn't enumerate methods via {@code
 * KafkaListenerEndpointRegistry}: once a {@code @KafkaListener} method is
 * registered, the registry exposes only the resulting {@code
 * MessageListenerContainer}s, not the original bean/{@link Method}/
 * annotations that produced them - there's no public API to walk back from
 * a container to its annotated method. Instead, every singleton bean's
 * (proxy-unwrapped via {@link ClassUtils#getUserClass}) declared methods are
 * scanned directly for the {@code @Idempotent} + {@code @KafkaListener}
 * pair - the same annotation-driven discovery {@code
 * KafkaListenerAnnotationBeanPostProcessor} itself performs internally to
 * find {@code @KafkaListener} methods in the first place. Every registered
 * bean definition is scanned by <em>type</em> (singleton or not - a
 * prototype-scoped {@code @KafkaListener} bean is still processed by {@code
 * KafkaListenerAnnotationBeanPostProcessor} once per instantiation, so it
 * needs the same startup check), never by eagerly instantiating one just to
 * inspect it ({@code allowFactoryBeanInit=false} throughout). Runs once,
 * after every singleton has finished initializing (by which point {@code
 * @KafkaListener} processing - a {@code BeanPostProcessor} callback that
 * runs per-bean, during creation - has already happened for all of them).
 */
public class KafkaIdempotentListenerValidator implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(KafkaIdempotentListenerValidator.class);

    private final ConfigurableListableBeanFactory beanFactory;
    private final IdempotencyProperties properties;

    public KafkaIdempotentListenerValidator(ConfigurableListableBeanFactory beanFactory, IdempotencyProperties properties) {
        this.beanFactory = beanFactory;
        this.properties = properties;
    }

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
        // Reached only if every @Idempotent @KafkaListener method above passed validation -
        // any failure throws IllegalStateException and aborts startup instead.
        log.info("Idempotency Kafka configuration valid: {} @Idempotent @KafkaListener method(s) validated", validatedCount.get());
    }

    private void validateIfIdempotentListener(Method method, Map<String, IdempotencyStore> storesByQualifier, AtomicInteger validatedCount) {
        KafkaListener kafkaListener = method.getAnnotation(KafkaListener.class);
        Idempotent annotation = method.getAnnotation(Idempotent.class);
        if (kafkaListener == null || annotation == null) {
            return;
        }
        validateWhenInProgress(method, annotation);
        validateSignature(method);
        validateKeyStrategy(method, annotation);
        validateTtl(method, annotation);
        validateStore(method, annotation, storesByQualifier);
        validatedCount.incrementAndGet();
        log.info("Idempotent Kafka listener: {} strategy={}={} store={}", describe(method),
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
            throw new IllegalStateException(describe(method) + ": whenInProgress=WAIT is not supported on a @KafkaListener method "
                    + "(blocking a listener container thread risks missing max.poll.interval.ms and triggering a partition "
                    + "rebalance) - set whenInProgress=REJECT on @Idempotent or idempotency.default-when-in-progress");
        }
    }

    private void validateSignature(Method method) {
        // The advice resolves the key and fingerprint from the raw ConsumerRecord
        // (KafkaIdempotencyAdvice.consumerRecordOf) - a POJO/@Payload-only listener
        // has nothing to resolve from, so it would throw at the first delivery. Fail
        // here at startup instead, matching the fail-fast promise of every other
        // check in this validator.
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (ConsumerRecord.class.isAssignableFrom(parameterType)) {
                return;
            }
        }
        throw new IllegalStateException(describe(method) + ": must accept a " + ConsumerRecord.class.getName()
                + " parameter for key resolution (a POJO/@Payload-only @KafkaListener is not supported)");
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

    private static String describe(Method method) {
        return method.getDeclaringClass().getSimpleName() + "." + method.getName() + "()";
    }
}
