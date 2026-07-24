package io.adzubla.blocks.idempotency.messaging.jms;

import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.messaging.core.AbstractMessagingIdempotencyAdvice;
import io.adzubla.blocks.idempotency.messaging.core.MessageDelivery;
import io.adzubla.blocks.idempotency.messaging.jms.key.JmsHeaderKeyStrategy;
import jakarta.jms.BytesMessage;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Intercepts every {@code @Idempotent} + {@code @JmsListener} method via
 * Spring AOP - the same {@code @Around}-advice mechanism {@code
 * @Transactional} already uses on listener methods (no buffered-request
 * wrapping needed, unlike {@code web.IdempotencyFilter}/{@code
 * CachedBodyHttpServletRequest} - the message payload already arrives as a
 * plain method argument).
 *
 * <p>The broker-neutral decision skeleton (key resolution, fingerprinting, the
 * reserve/complete/release flow, and the full PRD §5 decision table) lives in
 * {@link AbstractMessagingIdempotencyAdvice}, shared with the Kafka and
 * RabbitMQ modules since Slice 048. This class supplies only the JMS-specific
 * seams via a {@link JmsMessageDelivery}: reading the header/body off a {@link
 * Message}, and the two terminal actions whose mechanism is JMS-specific.
 *
 * <p><b>Dead-letter</b> (collision, missing/invalid key - PRD §5 terminal
 * cases no consumer-side retry resolves): unlike RabbitMQ's broker-native
 * reject-without-requeue, plain JMS has no primitive that skips a delivery
 * straight to a DLQ ahead of the broker's own redelivery-count-then-DLQ
 * policy, so - like the Kafka module's app-managed {@code
 * KafkaDeadLetterPublisher} - the delivery is explicitly republished to its
 * dead-letter destination via {@link JmsDeadLetterPublisher} and acked/skipped
 * (not left for the broker's own redelivery loop). <b>Fail-closed</b> ({@code
 * onStoreFailure=CLOSED} with the store down): transient infrastructure
 * trouble, not a poison message, so the listener is not invoked and the
 * delivery is left un-acked (thrown exception, causing a JMS session
 * rollback) for the broker to redeliver once the store has likely recovered -
 * "nack-with-backoff", relying entirely on the broker's own
 * redelivery/backoff policy, same as the Kafka module's fail-closed seam.
 * {@code onStoreFailure=OPEN} (the default) instead resolves to {@code
 * ProceedUnprotected} in the shared skeleton.
 */
@Aspect
public class JmsIdempotencyAdvice extends AbstractMessagingIdempotencyAdvice {

    private static final Logger log = LoggerFactory.getLogger(JmsIdempotencyAdvice.class);

    private final JmsDeadLetterPublisher deadLetterPublisher;

    public JmsIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties,
            JmsDeadLetterPublisher deadLetterPublisher) {
        super(engineRegistry, properties);
        this.deadLetterPublisher = deadLetterPublisher;
    }

    @Around("@annotation(io.adzubla.blocks.idempotency.annotation.Idempotent) "
            + "&& @annotation(org.springframework.jms.annotation.JmsListener)")
    public Object aroundIdempotentListener(ProceedingJoinPoint joinPoint) throws Throwable {
        return handle(joinPoint);
    }

    @Override
    protected MessageDelivery deliveryOf(ProceedingJoinPoint joinPoint, Method method) {
        return new JmsMessageDelivery(messageOf(joinPoint.getArgs(), method), listenerIdOf(method));
    }

    /** JMS seam: reads header/body off a {@link Message} and routes terminal deliveries to the dead-letter destination. */
    private final class JmsMessageDelivery implements MessageDelivery {

        private final Message message;
        private final String listenerId;

        private JmsMessageDelivery(Message message, String listenerId) {
            this.message = message;
            this.listenerId = listenerId;
        }

        @Override
        public String destination() {
            try {
                Destination destination = message.getJMSDestination();
                if (destination instanceof Queue queue) {
                    return queue.getQueueName();
                }
                if (destination instanceof Topic topic) {
                    return topic.getTopicName();
                }
                return String.valueOf(destination);
            } catch (JMSException e) {
                throw wrap("read JMS destination", e);
            }
        }

        @Override
        public String listenerId() {
            return listenerId;
        }

        @Override
        public byte[] body() {
            try {
                if (message instanceof TextMessage text) {
                    String payload = text.getText();
                    return payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8);
                }
                if (message instanceof BytesMessage bytes) {
                    byte[] buffer = new byte[(int) bytes.getBodyLength()];
                    bytes.readBytes(buffer);
                    // readBytes advances the read cursor; reset it so a listener that reads the
                    // same BytesMessage body after this fingerprinting read still sees the payload.
                    bytes.reset();
                    return buffer;
                }
                return new byte[0];
            } catch (JMSException e) {
                throw wrap("read JMS message body", e);
            }
        }

        @Override
        public Optional<String> resolveHeaderKey(String headerName) {
            return JmsHeaderKeyStrategy.resolve(message, headerName);
        }

        @Override
        public Object deadLetter(String reason, String value) {
            String destination = destination();
            log.debug("Idempotency {} - routing to dead-letter destination: destination={} listener={} key={}", reason, destination,
                    listenerId, value);
            deadLetterPublisher.publish(message, destination);
            return null;
        }

        @Override
        public Object failClosed() {
            String destination = destination();
            log.warn("Idempotency store unavailable (onStoreFailure=CLOSED) - listener not invoked, message left un-acked "
                    + "for broker redelivery: destination={} listener={}", destination, listenerId);
            throw new IllegalStateException("Idempotency store unavailable (onStoreFailure=CLOSED) for destination=" + destination
                    + " listener=" + listenerId + " - message not acked, awaiting broker redelivery");
        }

        private static JMSRuntimeException wrap(String action, JMSException e) {
            return new JMSRuntimeException("Failed to " + action + " for idempotency key resolution", e.getErrorCode(), e);
        }
    }

    private static Message messageOf(Object[] args, Method method) {
        for (Object arg : args) {
            if (arg instanceof Message message) {
                return message;
            }
        }
        throw new IllegalStateException(
                "@Idempotent @JmsListener method " + method.getDeclaringClass().getSimpleName() + "." + method.getName()
                        + "() must accept a jakarta.jms.Message parameter for key resolution");
    }

    private static String listenerIdOf(Method method) {
        String id = method.getAnnotation(JmsListener.class).id();
        return id.isEmpty() ? method.getDeclaringClass().getName() + "#" + method.getName() : id;
    }
}
