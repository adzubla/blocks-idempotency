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
 * reserve/complete/release flow, and the decision table) lives in {@link
 * AbstractMessagingIdempotencyAdvice}, shared with the Kafka and RabbitMQ
 * modules since Slice 048. This class supplies only the JMS-specific seams via
 * a {@link JmsMessageDelivery}: reading the header/body off a {@link Message}.
 *
 * <p><b>Slice 049 scope (this foundation):</b> the happy path - {@code Proceed}
 * (execute, then complete with {@code CachedResponse.empty()} - v1 is
 * dedupe-only, see ADR 0004), the completed-duplicate ack-and-skip case, and
 * fail-open ({@code onStoreFailure=OPEN}) pass-through. The terminal actions
 * the shared skeleton delegates to {@link MessageDelivery#deadLetter} /
 * {@link MessageDelivery#failClosed} - collision, missing/invalid key, and
 * fail-closed - are not yet mapped onto a JMS-native primitive and throw a
 * clear "not yet supported" error until Slice 050 (JMS action-mapping edge
 * cases), in the same spirit as the RabbitMQ/Kafka foundation slices, which
 * left their own non-happy-path decisions to a follow-up.
 */
@Aspect
public class JmsIdempotencyAdvice extends AbstractMessagingIdempotencyAdvice {

    public JmsIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties) {
        super(engineRegistry, properties);
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

    /** JMS seam: reads header/body off a {@link Message}. Terminal actions are deferred to Slice 050. */
    private static final class JmsMessageDelivery implements MessageDelivery {

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
            throw notYetSupported(reason, value);
        }

        @Override
        public Object failClosed() {
            throw notYetSupported("store unavailable (onStoreFailure=CLOSED)", "n/a");
        }

        /**
         * Slice 049 is the happy-path foundation; mapping a terminal delivery onto a JMS-native
         * primitive (broker redelivery/DLQ) is Slice 050. Until then these paths throw with a clear
         * message rather than being silently mishandled - matching the RabbitMQ foundation's
         * "not yet handled" catch-all.
         */
        private IllegalStateException notYetSupported(String reason, String value) {
            return new IllegalStateException("Idempotency " + reason + " is not yet handled for @JmsListener (Slice 049 is "
                    + "happy-path only; JMS action mapping is Slice 050): destination=" + destination() + " listener=" + listenerId
                    + " key=" + value);
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
