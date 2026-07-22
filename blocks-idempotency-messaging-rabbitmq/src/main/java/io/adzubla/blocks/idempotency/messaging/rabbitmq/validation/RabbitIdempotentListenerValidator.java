package io.adzubla.blocks.idempotency.messaging.rabbitmq.validation;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.IdempotencyStoreQualifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fails application startup with a clear message if any {@code @Idempotent}
 * {@code @RabbitListener} method is misconfigured: the same {@code
 * header}/{@code fieldPath}/{@code ttl}/{@code store} checks {@code
 * IdempotentHandlerValidator} enforces for HTTP and the Kafka module's own
 * {@code KafkaIdempotentListenerValidator} enforces for Kafka (no shared
 * helper - the checks are re-implemented directly here, mirroring rather
 * than reusing either), plus {@link #validateSignature a check that the
 * method accepts a {@link org.springframework.amqp.core.Message} parameter}
 * (the advice resolves the key and fingerprint from it, so a POJO/{@code
 * @Payload}-only listener would otherwise fail only at the first delivery),
 * plus one messaging-specific rule from {@code
 * docs/adr/0005-messaging-wait-disabled.md}: {@link #validateWhenInProgress
 * whenInProgress=WAIT} is rejected outright - only {@code REJECT} is
 * supported for v1, since blocking a listener container thread in {@code
 * store.await()} risks the container treating the consumer as stalled, a
 * worse failure mode than the narrow correctness fallback WAIT provides,
 * and is redundant given RabbitMQ's own broker-native redelivery.
 *
 * <p>Like the Kafka validator, this doesn't enumerate methods via a
 * listener-endpoint registry: once a {@code @RabbitListener} method is
 * registered, the registry exposes only the resulting {@code
 * MessageListenerContainer}s, not the original bean/{@link Method}/
 * annotations that produced them. Instead, every singleton bean's
 * (proxy-unwrapped via {@link ClassUtils#getUserClass}) declared methods are
 * scanned directly for the {@code @Idempotent} + {@code @RabbitListener}
 * pair - the same annotation-driven discovery {@code
 * RabbitListenerAnnotationBeanPostProcessor} itself performs internally to
 * find {@code @RabbitListener} methods in the first place. Every registered
 * bean definition is scanned by <em>type</em> (singleton or not - a
 * prototype-scoped {@code @RabbitListener} bean is still processed by
 * {@code RabbitListenerAnnotationBeanPostProcessor} once per instantiation,
 * so it needs the same startup check), never by eagerly instantiating one
 * just to inspect it ({@code allowFactoryBeanInit=false} throughout). Runs
 * once, after every singleton has finished initializing (by which point
 * {@code @RabbitListener} processing - a {@code BeanPostProcessor} callback
 * that runs per-bean, during creation - has already happened for all of
 * them).
 */
public class RabbitIdempotentListenerValidator implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(RabbitIdempotentListenerValidator.class);

    private final ConfigurableListableBeanFactory beanFactory;
    private final IdempotencyProperties properties;

    public RabbitIdempotentListenerValidator(ConfigurableListableBeanFactory beanFactory, IdempotencyProperties properties) {
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
        // Reached only if every @Idempotent @RabbitListener method above passed validation -
        // any failure throws IllegalStateException and aborts startup instead.
        log.info("Idempotency RabbitMQ configuration valid: {} @Idempotent @RabbitListener method(s) validated", validatedCount.get());
    }

    private void validateIfIdempotentListener(Method method, Map<String, IdempotencyStore> storesByQualifier, AtomicInteger validatedCount) {
        RabbitListener rabbitListener = method.getAnnotation(RabbitListener.class);
        Idempotent annotation = method.getAnnotation(Idempotent.class);
        if (rabbitListener == null || annotation == null) {
            return;
        }
        validateWhenInProgress(method, annotation);
        validateSignature(method);
        validateKeyStrategy(method, annotation);
        validateTtl(method, annotation);
        validateStore(method, annotation, storesByQualifier);
        validatedCount.incrementAndGet();
        log.info("Idempotent RabbitMQ listener: {} strategy={}={} store={}", describe(method),
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
            throw new IllegalStateException(describe(method) + ": whenInProgress=WAIT is not supported on a @RabbitListener method "
                    + "(blocking a listener container thread in store.await() risks the container treating the consumer as "
                    + "stalled) - set whenInProgress=REJECT on @Idempotent or idempotency.default-when-in-progress");
        }
    }

    private void validateSignature(Method method) {
        // The advice resolves the key and fingerprint from the raw AMQP Message
        // (RabbitIdempotencyAdvice.messageOf) - a POJO/@Payload-only listener has
        // nothing to resolve from, so it would throw at the first delivery. Fail
        // here at startup instead, matching the fail-fast promise of every other
        // check in this validator.
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (Message.class.isAssignableFrom(parameterType)) {
                return;
            }
        }
        throw new IllegalStateException(describe(method) + ": must accept a " + Message.class.getName()
                + " parameter for key resolution (a POJO/@Payload-only @RabbitListener is not supported)");
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
