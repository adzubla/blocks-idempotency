package io.adzubla.blocks.idempotency.messaging.jms.validation;

import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.messaging.core.validation.AbstractMessagingListenerValidator;
import jakarta.jms.Message;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.jms.annotation.JmsListener;

import java.lang.reflect.Method;

/**
 * JMS-specific specialization of {@link AbstractMessagingListenerValidator}
 * (Slice 051, mirroring the Kafka Slice 040 / RabbitMQ Slice 045 validators
 * onto the shared base from Slice 048): the shared {@code header}/{@code
 * fieldPath}/{@code ttl}/{@code store}/WAIT checks and the bean-scanning
 * mechanism live in the base; this class supplies only the JMS-specific seams
 * - the {@code @JmsListener} marker, the {@link Message} parameter
 * requirement (the advice resolves the key and fingerprint from it, so a
 * POJO/{@code @Payload}-only listener would otherwise fail only at the first
 * delivery), and the WAIT-rejection reason from {@code
 * docs/adr/0005-messaging-wait-disabled.md} (blocking a listener container
 * thread in {@code store.await()} risks similar container-level disruption to
 * a Kafka rebalance or a stalled RabbitMQ consumer, and is redundant given
 * JMS's own broker-native redelivery).
 */
public class JmsIdempotentListenerValidator extends AbstractMessagingListenerValidator {

    public JmsIdempotentListenerValidator(ConfigurableListableBeanFactory beanFactory, IdempotencyProperties properties) {
        super(beanFactory, properties);
    }

    @Override
    protected boolean isListenerMethod(Method method) {
        return method.isAnnotationPresent(JmsListener.class);
    }

    @Override
    protected void validateSignature(Method method) {
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (Message.class.isAssignableFrom(parameterType)) {
                return;
            }
        }
        throw new IllegalStateException(describe(method) + ": must accept a " + Message.class.getName()
                + " parameter for key resolution (a POJO/@Payload-only @JmsListener is not supported)");
    }

    @Override
    protected String waitNotSupportedMessage(Method method) {
        return describe(method) + ": whenInProgress=WAIT is not supported on a @JmsListener method "
                + "(blocking a listener container thread in store.await() risks similar container-level disruption to a "
                + "Kafka rebalance or a stalled RabbitMQ consumer, and is redundant given JMS's own broker-native redelivery) "
                + "- set whenInProgress=REJECT on @Idempotent or idempotency.default-when-in-progress";
    }

    @Override
    protected String brokerName() {
        return "JMS";
    }
}
