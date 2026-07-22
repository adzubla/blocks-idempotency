package io.adzubla.blocks.idempotency.messaging.rabbitmq;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.EngineDecision;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
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
 * without re-invoking the listener. {@link EngineDecision.Collision}/{@link
 * EngineDecision.Reject}/{@link EngineDecision.FailClosed} are out of scope
 * for this slice (see Slice 044 for their broker-specific dead-letter/
 * ack-skip/nack actions) and for now simply propagate as a thrown exception,
 * which Spring AMQP's default listener error handling treats as a failed
 * delivery (nacked/requeued or routed to its own dead-letter queue,
 * depending on the container's configured error handler).
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
                throw new IllegalStateException(
                        "Idempotency key missing but required: queue=" + queue + " listener=" + listenerIdOf(method));
            }
            // keyRequired=false: protection is a client opt-in - pass through unprotected, nothing cached.
            return joinPoint.proceed();
        }
        if (!KeyFormat.isValid(rawKey.get(), properties.getKey().getMaxLength())) {
            throw new IllegalStateException(
                    "Idempotency key value invalid (size/charset): queue=" + queue + " listener=" + listenerIdOf(method));
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
        throw new IllegalStateException(
                "Idempotency decision " + decision + " is not yet handled for queue=" + queue + " listener=" + listenerIdOf(method));
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
