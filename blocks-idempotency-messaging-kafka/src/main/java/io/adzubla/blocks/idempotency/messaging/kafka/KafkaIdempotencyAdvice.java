package io.adzubla.blocks.idempotency.messaging.kafka;

import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.messaging.core.AbstractMessagingIdempotencyAdvice;
import io.adzubla.blocks.idempotency.messaging.core.MessageDelivery;
import io.adzubla.blocks.idempotency.messaging.kafka.key.KafkaHeaderKeyStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Intercepts every {@code @Idempotent} + {@code @KafkaListener} method via
 * Spring AOP - the same {@code @Around}-advice mechanism {@code
 * @Transactional} already uses on listener methods (no buffered-request
 * wrapping needed, unlike {@code web.IdempotencyFilter}/{@code
 * CachedBodyHttpServletRequest} - the message payload already arrives as a
 * plain method argument).
 *
 * <p>The broker-neutral decision skeleton (key resolution, fingerprinting, the
 * reserve/complete/release flow, and the full PRD §5 decision table) lives in
 * {@link AbstractMessagingIdempotencyAdvice}, shared with the RabbitMQ module
 * since Slice 048. This class supplies only the Kafka-specific seams via a
 * {@link KafkaMessageDelivery}: reading the header/body off a {@link
 * ConsumerRecord}, and the two terminal actions whose mechanism is
 * Kafka-specific.
 *
 * <p><b>Dead-letter</b> (collision, missing/invalid key - PRD §5 terminal
 * cases no consumer-side retry resolves): the delivery is republished to its
 * dead-letter topic via {@link KafkaDeadLetterPublisher} and acked/skipped.
 * <b>Fail-closed</b> ({@code onStoreFailure=CLOSED} with the store down):
 * transient infrastructure trouble, not a poison message, so the listener is
 * not invoked and the delivery is left un-acked (thrown exception) for the
 * broker to redeliver once the store has likely recovered. {@code
 * onStoreFailure=OPEN} (the default) instead resolves to {@code
 * ProceedUnprotected} in the shared skeleton.
 *
 * <p>Whether a thrown exception actually leaves the delivery un-acked - and
 * how soon it's retried ("nack-with-backoff", PRD §5) rather than exhausting a
 * default retry budget and being skipped anyway - depends entirely on the
 * {@code ConcurrentKafkaListenerContainerFactory}'s error handler, which this
 * module doesn't own or configure (see {@code KafkaIdempotencyEndToEndTest}'s
 * test app, which wires a {@code DefaultErrorHandler} with an unlimited {@code
 * FixedBackOff} to prove the mechanism end-to-end). An application using this
 * posture in production needs its own container-factory error handler
 * configured accordingly.
 */
@Aspect
public class KafkaIdempotencyAdvice extends AbstractMessagingIdempotencyAdvice {

    private static final Logger log = LoggerFactory.getLogger(KafkaIdempotencyAdvice.class);

    private final KafkaDeadLetterPublisher deadLetterPublisher;

    public KafkaIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties,
            KafkaDeadLetterPublisher deadLetterPublisher) {
        super(engineRegistry, properties);
        this.deadLetterPublisher = deadLetterPublisher;
    }

    @Around("@annotation(io.adzubla.blocks.idempotency.annotation.Idempotent) "
            + "&& @annotation(org.springframework.kafka.annotation.KafkaListener)")
    public Object aroundIdempotentListener(ProceedingJoinPoint joinPoint) throws Throwable {
        return handle(joinPoint);
    }

    @Override
    protected MessageDelivery deliveryOf(ProceedingJoinPoint joinPoint, Method method) {
        return new KafkaMessageDelivery(consumerRecordOf(joinPoint.getArgs(), method), listenerIdOf(method));
    }

    /** Kafka seam: reads header/body off a {@link ConsumerRecord} and routes terminal deliveries to the dead-letter topic. */
    private final class KafkaMessageDelivery implements MessageDelivery {

        private final ConsumerRecord<?, ?> record;
        private final String listenerId;

        private KafkaMessageDelivery(ConsumerRecord<?, ?> record, String listenerId) {
            this.record = record;
            this.listenerId = listenerId;
        }

        @Override
        public String destination() {
            return record.topic();
        }

        @Override
        public String listenerId() {
            return listenerId;
        }

        @Override
        public byte[] body() {
            Object value = record.value();
            if (value instanceof byte[] bytes) {
                return bytes;
            }
            if (value instanceof String s) {
                return s.getBytes(StandardCharsets.UTF_8);
            }
            return new byte[0];
        }

        @Override
        public Optional<String> resolveHeaderKey(String headerName) {
            return KafkaHeaderKeyStrategy.resolve(record.headers(), headerName);
        }

        @Override
        public Object deadLetter(String reason, String value) {
            log.debug("Idempotency {} - routing to dead-letter: topic={} listener={} key={}", reason, record.topic(), listenerId, value);
            deadLetterPublisher.publish(record);
            return null;
        }

        @Override
        public Object failClosed() {
            log.warn("Idempotency store unavailable (onStoreFailure=CLOSED) - listener not invoked, message left un-acked "
                    + "for broker redelivery: topic={} listener={}", record.topic(), listenerId);
            throw new IllegalStateException("Idempotency store unavailable (onStoreFailure=CLOSED) for topic=" + record.topic()
                    + " listener=" + listenerId + " - message not acked, awaiting broker redelivery");
        }
    }

    private static ConsumerRecord<?, ?> consumerRecordOf(Object[] args, Method method) {
        for (Object arg : args) {
            if (arg instanceof ConsumerRecord<?, ?> record) {
                return record;
            }
        }
        throw new IllegalStateException(
                "@Idempotent @KafkaListener method " + method.getDeclaringClass().getSimpleName() + "." + method.getName()
                        + "() must accept a ConsumerRecord<?, ?> parameter for key resolution");
    }

    private static String listenerIdOf(Method method) {
        String id = method.getAnnotation(KafkaListener.class).id();
        return id.isEmpty() ? method.getDeclaringClass().getName() + "#" + method.getName() : id;
    }
}
