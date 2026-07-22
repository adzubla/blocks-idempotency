package io.adzubla.blocks.idempotency.messaging.rabbitmq;

import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.messaging.core.AbstractMessagingIdempotencyAdvice;
import io.adzubla.blocks.idempotency.messaging.core.MessageDelivery;
import io.adzubla.blocks.idempotency.messaging.rabbitmq.key.RabbitHeaderKeyStrategy;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Intercepts every {@code @Idempotent} + {@code @RabbitListener} method via
 * Spring AOP - the same {@code @Around}-advice mechanism {@code
 * @Transactional} already uses on listener methods (no buffered-request
 * wrapping needed, unlike {@code web.IdempotencyFilter}/{@code
 * CachedBodyHttpServletRequest} - the message payload already arrives as a
 * plain method argument).
 *
 * <p>The broker-neutral decision skeleton (key resolution, fingerprinting, the
 * reserve/complete/release flow, and the full PRD §5 decision table) lives in
 * {@link AbstractMessagingIdempotencyAdvice}, shared with the Kafka module
 * since Slice 048. This class supplies only the RabbitMQ-specific seams via a
 * {@link RabbitMessageDelivery}: reading the header/body off a {@link Message},
 * and the two terminal actions whose mechanism is RabbitMQ-specific.
 *
 * <p>Unlike the Kafka module's app-managed {@code KafkaDeadLetterPublisher},
 * RabbitMQ carries dead-lettering as a broker-native feature, so this advice
 * only signals the outcome rather than routing itself. <b>Dead-letter</b>
 * (collision, missing/invalid key) throws {@link
 * AmqpRejectAndDontRequeueException}, which the container recognizes and
 * rejects (requeue=false) - routed onward to whatever dead-letter
 * exchange/queue the application configured on the queue itself (this module
 * owns neither). <b>Fail-closed</b> ({@code onStoreFailure=CLOSED} with the
 * store down) also throws {@link AmqpRejectAndDontRequeueException} - the same
 * {@code requeue=false} primitive as dead-lettering, per the PRD's "{@code
 * channel.basicNack} (requeue=false, relying on a retry/backoff policy)"
 * wording - but with a distinct log message, since which queue this lands in
 * (a genuinely terminal DLQ vs. a TTL-holding retry queue that dead-letters
 * back to the original queue once the store has likely recovered - see {@code
 * RabbitIdempotencyEndToEndTest}) is an operational decision made via the
 * queue's own dead-letter configuration rather than in code. {@code
 * onStoreFailure=OPEN} (the default) instead resolves to {@code
 * ProceedUnprotected} in the shared skeleton.
 */
@Aspect
public class RabbitIdempotencyAdvice extends AbstractMessagingIdempotencyAdvice {

    private static final Logger log = LoggerFactory.getLogger(RabbitIdempotencyAdvice.class);

    public RabbitIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties) {
        super(engineRegistry, properties);
    }

    @Around("@annotation(io.adzubla.blocks.idempotency.annotation.Idempotent) "
            + "&& @annotation(org.springframework.amqp.rabbit.annotation.RabbitListener)")
    public Object aroundIdempotentListener(ProceedingJoinPoint joinPoint) throws Throwable {
        return handle(joinPoint);
    }

    @Override
    protected MessageDelivery deliveryOf(ProceedingJoinPoint joinPoint, Method method) {
        return new RabbitMessageDelivery(messageOf(joinPoint.getArgs(), method), listenerIdOf(method));
    }

    /** RabbitMQ seam: reads header/body off a {@link Message} and rejects terminal deliveries without requeue for broker dead-lettering. */
    private static final class RabbitMessageDelivery implements MessageDelivery {

        private final Message message;
        private final String listenerId;

        private RabbitMessageDelivery(Message message, String listenerId) {
            this.message = message;
            this.listenerId = listenerId;
        }

        @Override
        public String destination() {
            return message.getMessageProperties().getConsumerQueue();
        }

        @Override
        public String listenerId() {
            return listenerId;
        }

        @Override
        public byte[] body() {
            return message.getBody();
        }

        @Override
        public Optional<String> resolveHeaderKey(String headerName) {
            return RabbitHeaderKeyStrategy.resolve(message.getMessageProperties().getHeaders(), headerName);
        }

        @Override
        public Object deadLetter(String reason, String value) {
            String queue = destination();
            log.debug("Idempotency {} - rejecting without requeue for broker dead-lettering: queue={} listener={} key={}", reason, queue,
                    listenerId, value);
            throw new AmqpRejectAndDontRequeueException(
                    "Idempotency " + reason + ": queue=" + queue + " listener=" + listenerId + " key=" + value);
        }

        @Override
        public Object failClosed() {
            String queue = destination();
            log.warn("Idempotency store unavailable (onStoreFailure=CLOSED) - listener not invoked, message rejected without "
                    + "requeue for the queue's own configured retry/backoff policy: queue={} listener={}", queue, listenerId);
            throw new AmqpRejectAndDontRequeueException("Idempotency store unavailable (onStoreFailure=CLOSED) for queue=" + queue
                    + " listener=" + listenerId + " - message rejected without requeue, awaiting broker-configured retry");
        }
    }

    private static Message messageOf(Object[] args, Method method) {
        for (Object arg : args) {
            if (arg instanceof Message message) {
                return message;
            }
        }
        throw new IllegalStateException(
                "@Idempotent @RabbitListener method " + method.getDeclaringClass().getSimpleName() + "." + method.getName()
                        + "() must accept a org.springframework.amqp.core.Message parameter for key resolution");
    }

    private static String listenerIdOf(Method method) {
        String id = method.getAnnotation(RabbitListener.class).id();
        return id.isEmpty() ? method.getDeclaringClass().getName() + "#" + method.getName() : id;
    }
}
