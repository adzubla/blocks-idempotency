package io.adzubla.blocks.idempotency.messaging.kafka.validation;

import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.messaging.core.validation.AbstractMessagingListenerValidator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.reflect.Method;

/**
 * Kafka-specific specialization of {@link AbstractMessagingListenerValidator}
 * (Slice 040, refactored onto the shared base in Slice 048): the shared {@code
 * header}/{@code fieldPath}/{@code ttl}/{@code store}/WAIT checks and the
 * bean-scanning mechanism live in the base; this class supplies only the
 * Kafka-specific seams - the {@code @KafkaListener} marker, the {@link
 * ConsumerRecord} parameter requirement (the advice resolves the key and
 * fingerprint from it, so a POJO/{@code @Payload}-only listener would
 * otherwise fail only at the first delivery), and the WAIT-rejection reason
 * from {@code docs/adr/0005-messaging-wait-disabled.md} (blocking a listener
 * container thread in {@code store.await()} risks missing {@code
 * max.poll.interval.ms} and triggering a partition rebalance).
 */
public class KafkaIdempotentListenerValidator extends AbstractMessagingListenerValidator {

    public KafkaIdempotentListenerValidator(ConfigurableListableBeanFactory beanFactory, IdempotencyProperties properties) {
        super(beanFactory, properties);
    }

    @Override
    protected boolean isListenerMethod(Method method) {
        return method.isAnnotationPresent(KafkaListener.class);
    }

    @Override
    protected void validateSignature(Method method) {
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (ConsumerRecord.class.isAssignableFrom(parameterType)) {
                return;
            }
        }
        throw new IllegalStateException(describe(method) + ": must accept a " + ConsumerRecord.class.getName()
                + " parameter for key resolution (a POJO/@Payload-only @KafkaListener is not supported)");
    }

    @Override
    protected String waitNotSupportedMessage(Method method) {
        return describe(method) + ": whenInProgress=WAIT is not supported on a @KafkaListener method "
                + "(blocking a listener container thread risks missing max.poll.interval.ms and triggering a partition "
                + "rebalance) - set whenInProgress=REJECT on @Idempotent or idempotency.default-when-in-progress";
    }

    @Override
    protected String brokerName() {
        return "Kafka";
    }
}
