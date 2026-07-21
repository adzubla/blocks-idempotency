package io.adzubla.blocks.idempotency.messaging.kafka;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.EngineDecision;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.engine.RejectReason;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.key.BodyFieldKeyStrategy;
import io.adzubla.blocks.idempotency.key.KeyFormat;
import io.adzubla.blocks.idempotency.messaging.kafka.key.KafkaEffectiveKeyFactory;
import io.adzubla.blocks.idempotency.messaging.kafka.key.KafkaHeaderKeyStrategy;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.policy.IdempotencyPolicy;
import io.adzubla.blocks.idempotency.policy.PolicyResolver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
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
 * plain method argument). Mirrors {@code web.IdempotencyInterceptor}'s
 * preHandle/afterCompletion pair, collapsed into a single around-advice
 * since there's no separate dispatch phase to hook into.
 *
 * <p>Slice 035 scope: {@link EngineDecision.Proceed} (execute, then complete
 * with {@link CachedResponse#empty()} - v1 is dedupe-only, see ADR 0004) and
 * the completed-duplicate case (always {@link EngineDecision.Unavailable},
 * never {@code Replay}, since the sentinel response is always bodyless - see
 * {@link IdempotencyEngine#complete}'s javadoc) which is acked and skipped
 * without re-invoking the listener. Slice 036 adds {@link
 * EngineDecision.Collision}: a key reused with a different payload is a
 * producer bug or poison message no consumer-side retry resolves, so it's
 * routed to the dead-letter topic via {@link KafkaDeadLetterPublisher}
 * instead of invoking the listener (PRD §5). Slice 037 adds {@link
 * EngineDecision.Reject} with {@link RejectReason#IN_PROGRESS}: a second,
 * concurrent delivery of the same key (two partitions/consumers racing, or a
 * rebalance re-delivering mid-processing) is acked and skipped without
 * re-invoking the listener - safe because the primary's own message is
 * natively redelivered by the broker if it fails, so the duplicate isn't
 * needed as a backup. {@code whenInProgress=WAIT} is disabled for v1 (ADR
 * 0005, enforced at startup in Slice 040), so {@link RejectReason#RELEASED}/
 * {@link RejectReason#TIMEOUT} (WAIT-only outcomes) aren't expected to reach
 * this advice - a stray occurrence still falls through to the catch-all
 * thrown exception below rather than being silently ack-skipped. Slice 038
 * adds {@link EngineDecision.FailClosed}: the store is unavailable and the
 * resolved posture is {@code onStoreFailure=CLOSED} - transient
 * infrastructure trouble, not a poison message, so the listener is not
 * invoked and the delivery is left un-acked (thrown exception, same as the
 * catch-all) for the broker to redeliver once the store has likely
 * recovered. {@code onStoreFailure=OPEN} (the default) instead resolves to
 * {@link EngineDecision.ProceedUnprotected} above, handled since Slice 035.
 *
 * <p>Whether a thrown exception actually leaves the delivery un-acked - and
 * how soon it's retried ("nack-with-backoff", PRD §5) rather than
 * exhausting a default retry budget and being skipped anyway - depends
 * entirely on the {@code ConcurrentKafkaListenerContainerFactory}'s error
 * handler, which this module doesn't own or configure (see {@code
 * KafkaIdempotencyEndToEndTest}'s test app, which wires a {@code
 * DefaultErrorHandler} with an unlimited {@code FixedBackOff} to prove the
 * mechanism end-to-end). An application using this posture in production
 * needs its own container-factory error handler configured accordingly.
 */
@Aspect
public class KafkaIdempotencyAdvice {

    private static final Logger log = LoggerFactory.getLogger(KafkaIdempotencyAdvice.class);

    private final IdempotencyEngineRegistry engineRegistry;
    private final IdempotencyProperties properties;
    private final KafkaDeadLetterPublisher deadLetterPublisher;

    public KafkaIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties,
            KafkaDeadLetterPublisher deadLetterPublisher) {
        this.engineRegistry = engineRegistry;
        this.properties = properties;
        this.deadLetterPublisher = deadLetterPublisher;
    }

    @Around("@annotation(io.adzubla.blocks.idempotency.annotation.Idempotent) "
            + "&& @annotation(org.springframework.kafka.annotation.KafkaListener)")
    public Object aroundIdempotentListener(ProceedingJoinPoint joinPoint) throws Throwable {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        Idempotent annotation = method.getAnnotation(Idempotent.class);
        ConsumerRecord<?, ?> record = consumerRecordOf(joinPoint.getArgs(), method);

        IdempotencyPolicy policy = PolicyResolver.resolve(annotation, properties);
        byte[] body = bodyOf(record);

        Optional<String> rawKey = annotation.header().isEmpty()
                ? BodyFieldKeyStrategy.resolve(body, annotation.fieldPath())
                : KafkaHeaderKeyStrategy.resolve(record.headers(), annotation.header());
        if (rawKey.isEmpty()) {
            if (policy.keyRequired()) {
                throw new IllegalStateException(
                        "Idempotency key missing but required: topic=" + record.topic() + " listener=" + listenerIdOf(method));
            }
            // keyRequired=false: protection is a client opt-in - pass through unprotected, nothing cached.
            return joinPoint.proceed();
        }
        if (!KeyFormat.isValid(rawKey.get(), properties.getKey().getMaxLength())) {
            throw new IllegalStateException(
                    "Idempotency key value invalid (size/charset): topic=" + record.topic() + " listener=" + listenerIdOf(method));
        }

        EffectiveKey key = KafkaEffectiveKeyFactory.create(record.topic(), listenerIdOf(method), rawKey.get());
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
            log.debug("Duplicate Kafka delivery acked without re-invoking the listener: topic={} listener={} key={}",
                    key.route(), key.handler(), key.value());
            return null;
        }
        if (decision instanceof EngineDecision.Collision) {
            log.debug("Idempotency collision - routing to dead-letter: topic={} listener={} key={}",
                    key.route(), key.handler(), key.value());
            deadLetterPublisher.publish(record);
            return null;
        }
        if (decision instanceof EngineDecision.Reject reject && reject.reason() == RejectReason.IN_PROGRESS) {
            log.debug("Concurrent duplicate delivery acked without invoking the listener: topic={} listener={} key={}",
                    key.route(), key.handler(), key.value());
            return null;
        }
        if (decision instanceof EngineDecision.FailClosed) {
            log.warn("Idempotency store unavailable (onStoreFailure=CLOSED) - listener not invoked, message left un-acked "
                    + "for broker redelivery: topic={} listener={} key={}", key.route(), key.handler(), key.value());
            throw new IllegalStateException("Idempotency store unavailable (onStoreFailure=CLOSED) for topic=" + record.topic()
                    + " listener=" + listenerIdOf(method) + " - message not acked, awaiting broker redelivery");
        }
        throw new IllegalStateException("Idempotency decision " + decision + " is not yet handled for topic="
                + record.topic() + " listener=" + listenerIdOf(method));
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

    private static byte[] bodyOf(ConsumerRecord<?, ?> record) {
        Object value = record.value();
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }
        return new byte[0];
    }

    private static String listenerIdOf(Method method) {
        String id = method.getAnnotation(KafkaListener.class).id();
        return id.isEmpty() ? method.getDeclaringClass().getName() + "#" + method.getName() : id;
    }
}
