package io.adzubla.blocks.idempotency.messaging.rabbitmq.validation;

import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.messaging.core.validation.AbstractMessagingListenerValidator;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.lang.reflect.Method;

/**
 * RabbitMQ-specific specialization of {@link AbstractMessagingListenerValidator}
 * (Slice 045, refactored onto the shared base in Slice 048): the shared {@code
 * header}/{@code fieldPath}/{@code ttl}/{@code store}/WAIT checks and the
 * bean-scanning mechanism live in the base; this class supplies only the
 * RabbitMQ-specific seams - the {@code @RabbitListener} marker, the {@link
 * Message} parameter requirement (the advice resolves the key and fingerprint
 * from it, so a POJO/{@code @Payload}-only listener would otherwise fail only
 * at the first delivery), and the WAIT-rejection reason from {@code
 * docs/adr/0005-messaging-wait-disabled.md} (blocking a listener container
 * thread in {@code store.await()} risks the container treating the consumer as
 * stalled, and is redundant given RabbitMQ's own broker-native redelivery).
 */
public class RabbitIdempotentListenerValidator extends AbstractMessagingListenerValidator {

    public RabbitIdempotentListenerValidator(ConfigurableListableBeanFactory beanFactory, IdempotencyProperties properties) {
        super(beanFactory, properties);
    }

    @Override
    protected boolean isListenerMethod(Method method) {
        return method.isAnnotationPresent(RabbitListener.class);
    }

    @Override
    protected void validateSignature(Method method) {
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (Message.class.isAssignableFrom(parameterType)) {
                return;
            }
        }
        throw new IllegalStateException(describe(method) + ": must accept a " + Message.class.getName()
                + " parameter for key resolution (a POJO/@Payload-only @RabbitListener is not supported)");
    }

    @Override
    protected String waitNotSupportedMessage(Method method) {
        return describe(method) + ": whenInProgress=WAIT is not supported on a @RabbitListener method "
                + "(blocking a listener container thread in store.await() risks the container treating the consumer as "
                + "stalled) - set whenInProgress=REJECT on @Idempotent or idempotency.default-when-in-progress";
    }

    @Override
    protected String brokerName() {
        return "RabbitMQ";
    }
}
