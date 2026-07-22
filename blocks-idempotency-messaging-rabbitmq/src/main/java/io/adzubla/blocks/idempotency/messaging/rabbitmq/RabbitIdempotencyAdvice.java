package io.adzubla.blocks.idempotency.messaging.rabbitmq;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.EngineDecision;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.engine.RejectReason;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.key.BodyFieldKeyStrategy;
import io.adzubla.blocks.idempotency.key.KeyFormat;
import io.adzubla.blocks.idempotency.messaging.rabbitmq.key.RabbitEffectiveKeyFactory;
import io.adzubla.blocks.idempotency.messaging.rabbitmq.key.RabbitHeaderKeyStrategy;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.policy.IdempotencyPolicy;
import io.adzubla.blocks.idempotency.policy.PolicyResolver;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
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
 * plain method argument). Mirrors {@code web.IdempotencyInterceptor}'s
 * preHandle/afterCompletion pair, collapsed into a single around-advice
 * since there's no separate dispatch phase to hook into, and the Kafka
 * module's {@code KafkaIdempotencyAdvice} (same foundation, Slice 035).
 *
 * <p>Slice 043 scope: {@link EngineDecision.Proceed} (execute, then complete
 * with {@link CachedResponse#empty()} - v1 is dedupe-only, see ADR 0004) and
 * the completed-duplicate case (always {@link EngineDecision.Unavailable},
 * never {@code Replay}, since the sentinel response is always bodyless - see
 * {@link IdempotencyEngine#complete}'s javadoc) which is acked and skipped
 * without re-invoking the listener.
 *
 * <p>Slice 044 adds the remaining rows of the PRD §5 action-mapping table,
 * each mapped onto a RabbitMQ-native primitive rather than an app-managed
 * publisher (unlike the Kafka module's {@code KafkaDeadLetterPublisher} -
 * RabbitMQ queues carry dead-lettering as a broker-native feature, so this
 * advice only needs to signal the outcome, not perform the routing itself):
 * {@link EngineDecision.Collision} (fingerprint mismatch - a producer bug or
 * poison message no consumer-side retry resolves) and a missing/invalid key
 * (evaluated before a decision is even requested, since it will never carry
 * a resolvable key) both throw {@link AmqpRejectAndDontRequeueException},
 * which the container's error handling recognizes and rejects
 * (requeue=false) without re-invoking the listener - routed onward to
 * whatever dead-letter exchange/queue the application configured on the
 * queue itself (this module owns neither). {@link EngineDecision.Reject}
 * with {@link RejectReason#IN_PROGRESS} (a second, concurrent delivery of
 * the same key) is acked and skipped without re-invoking the listener -
 * safe because the primary's own message is natively redelivered by the
 * broker if it fails, so the duplicate isn't needed as a backup. {@code
 * whenInProgress=WAIT} is disabled for v1 (ADR 0005, enforced at startup in
 * Slice 045), so {@link RejectReason#RELEASED}/{@link RejectReason#TIMEOUT}
 * (WAIT-only outcomes) aren't expected to reach this advice - a stray
 * occurrence still falls through to the catch-all thrown exception below
 * rather than being silently ack-skipped. {@link EngineDecision.FailClosed}
 * (the store is unavailable and the resolved posture is {@code
 * onStoreFailure=CLOSED}) is transient infrastructure trouble, not a poison
 * message, so it also throws {@link AmqpRejectAndDontRequeueException} - the
 * same {@code requeue=false} primitive as dead-lettering, per the PRD's
 * "{@code channel.basicNack} (requeue=false, relying on a retry/backoff
 * policy)" wording - but with a distinct log message, since which queue this
 * lands in (a genuinely terminal DLQ vs. a TTL-holding retry queue that
 * dead-letters back to the original queue once the store has likely
 * recovered - see {@code RabbitIdempotencyEndToEndTest}) is an operational
 * decision this module doesn't own, made via the queue's own dead-letter
 * configuration rather than in code. {@code onStoreFailure=OPEN} (the
 * default) instead resolves to {@link EngineDecision.ProceedUnprotected}
 * above, handled since Slice 043.
 */
@Aspect
public class RabbitIdempotencyAdvice {

    private static final Logger log = LoggerFactory.getLogger(RabbitIdempotencyAdvice.class);

    private final IdempotencyEngineRegistry engineRegistry;
    private final IdempotencyProperties properties;

    public RabbitIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties) {
        this.engineRegistry = engineRegistry;
        this.properties = properties;
    }

    @Around("@annotation(io.adzubla.blocks.idempotency.annotation.Idempotent) "
            + "&& @annotation(org.springframework.amqp.rabbit.annotation.RabbitListener)")
    public Object aroundIdempotentListener(ProceedingJoinPoint joinPoint) throws Throwable {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        Idempotent annotation = method.getAnnotation(Idempotent.class);
        Message message = messageOf(joinPoint.getArgs(), method);
        String queue = message.getMessageProperties().getConsumerQueue();

        IdempotencyPolicy policy = PolicyResolver.resolve(annotation, properties);
        byte[] body = message.getBody();

        Optional<String> rawKey = annotation.header().isEmpty()
                ? BodyFieldKeyStrategy.resolve(body, annotation.fieldPath())
                : RabbitHeaderKeyStrategy.resolve(message.getMessageProperties().getHeaders(), annotation.header());
        if (rawKey.isEmpty()) {
            if (policy.keyRequired()) {
                throw deadLetter("key missing but required", queue, listenerIdOf(method), "n/a");
            }
            // keyRequired=false: protection is a client opt-in - pass through unprotected, nothing cached.
            return joinPoint.proceed();
        }
        if (!KeyFormat.isValid(rawKey.get(), properties.getKey().getMaxLength())) {
            throw deadLetter("key value invalid (size/charset)", queue, listenerIdOf(method), rawKey.get());
        }

        EffectiveKey key = RabbitEffectiveKeyFactory.create(queue, listenerIdOf(method), rawKey.get());
        String fingerprint = Fingerprint.sha256(key.route(), key.handler(), body);

        IdempotencyEngine engine = engineRegistry.engine(policy.store());
        EngineDecision decision = engine.before(key, fingerprint, properties.getLockTtl(), policy.onStoreFailure(),
                policy.whenInProgress(), properties.getWaitTimeout());

        if (decision instanceof EngineDecision.Proceed proceed) {
            try {
                Object result = joinPoint.proceed();
                engine.complete(key, proceed.fenceToken(), CachedResponse.empty(), policy.ttl());
                return result;
            } catch (Throwable t) {
                engine.release(key, proceed.fenceToken());
                throw t;
            }
        }
        if (decision instanceof EngineDecision.ProceedUnprotected) {
            // The store is down and the posture is fail-open: run the listener unprotected.
            return joinPoint.proceed();
        }
        if (decision instanceof EngineDecision.Replay || decision instanceof EngineDecision.Unavailable) {
            log.debug("Duplicate RabbitMQ delivery acked without re-invoking the listener: queue={} listener={} key={}",
                    key.route(), key.handler(), key.value());
            return null;
        }
        if (decision instanceof EngineDecision.Collision) {
            throw deadLetter("collision (fingerprint mismatch)", key.route(), key.handler(), key.value());
        }
        if (decision instanceof EngineDecision.Reject reject && reject.reason() == RejectReason.IN_PROGRESS) {
            log.debug("Concurrent duplicate delivery acked without invoking the listener: queue={} listener={} key={}",
                    key.route(), key.handler(), key.value());
            return null;
        }
        if (decision instanceof EngineDecision.FailClosed) {
            log.warn("Idempotency store unavailable (onStoreFailure=CLOSED) - listener not invoked, message rejected without "
                    + "requeue for the queue's own configured retry/backoff policy: queue={} listener={} key={}", key.route(),
                    key.handler(), key.value());
            throw new AmqpRejectAndDontRequeueException("Idempotency store unavailable (onStoreFailure=CLOSED) for queue=" + queue
                    + " listener=" + listenerIdOf(method) + " - message rejected without requeue, awaiting broker-configured retry");
        }
        throw new IllegalStateException(
                "Idempotency decision " + decision + " is not yet handled for queue=" + queue + " listener=" + listenerIdOf(method));
    }

    /** Rejects the delivery without requeue (broker-native dead-lettering to whatever DLX/DLQ the queue is configured with). */
    private static AmqpRejectAndDontRequeueException deadLetter(String reason, String queue, String listener, String value) {
        log.debug("Idempotency {} - rejecting without requeue for broker dead-lettering: queue={} listener={} key={}", reason, queue,
                listener, value);
        return new AmqpRejectAndDontRequeueException(
                "Idempotency " + reason + ": queue=" + queue + " listener=" + listener + " key=" + value);
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
