package io.adzubla.blocks.idempotency.messaging.core;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.EngineDecision;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.engine.RejectReason;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.key.BodyFieldKeyStrategy;
import io.adzubla.blocks.idempotency.key.KeyFormat;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.policy.IdempotencyPolicy;
import io.adzubla.blocks.idempotency.policy.PolicyResolver;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Optional;

/**
 * The broker-neutral {@code @Around}-advice skeleton (before/complete/release
 * call sequence against the {@link IdempotencyEngine}, plus the full PRD §5
 * decision-to-action mapping) shared by every message-listener broker. A
 * concrete subclass in each broker module supplies the {@code @Aspect} +
 * {@code @Around} pointcut (which annotation pair to intercept) and adapts the
 * intercepted arguments to a {@link MessageDelivery} via {@link #deliveryOf};
 * everything below the seam - key resolution order, size/charset validation,
 * fingerprinting, the reserve/complete/release flow, and the decision table -
 * is identical across brokers and lives here (Slice 048, extracted from the
 * Kafka module's Slice 035-039 advice and the RabbitMQ module's Slice 043-044
 * advice, which had drifted into two copies of the same logic).
 *
 * <p>The two terminal actions whose <em>mechanism</em> genuinely differs
 * between brokers - dead-lettering a poison message and rejecting a
 * fail-closed delivery for broker redelivery - are delegated to {@link
 * MessageDelivery#deadLetter} / {@link MessageDelivery#failClosed}. {@code
 * whenInProgress=WAIT} is disabled for v1 (ADR 0005, enforced at startup by
 * {@code AbstractMessagingListenerValidator}), so {@link RejectReason#RELEASED}/
 * {@link RejectReason#TIMEOUT} (WAIT-only outcomes) aren't expected to reach
 * this advice - a stray occurrence falls through to the catch-all thrown
 * exception below rather than being silently ack-skipped.
 */
public abstract class AbstractMessagingIdempotencyAdvice {

    private static final Logger log = LoggerFactory.getLogger(AbstractMessagingIdempotencyAdvice.class);

    protected final IdempotencyEngineRegistry engineRegistry;
    protected final IdempotencyProperties properties;

    protected AbstractMessagingIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties) {
        this.engineRegistry = engineRegistry;
        this.properties = properties;
    }

    /**
     * Broker seam: adapt the intercepted invocation to a {@link MessageDelivery}. Called once per
     * delivery, before any engine interaction. Implementations extract the raw broker message from
     * {@code joinPoint.getArgs()} and resolve the listener id from {@code method}.
     */
    protected abstract MessageDelivery deliveryOf(ProceedingJoinPoint joinPoint, Method method);

    /**
     * Runs one intercepted {@code @Idempotent} listener delivery through the reserve/complete/release
     * flow and the PRD §5 decision table. A concrete broker subclass calls this from its {@code
     * @Around} advice method.
     */
    protected final Object handle(ProceedingJoinPoint joinPoint) throws Throwable {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        Idempotent annotation = method.getAnnotation(Idempotent.class);
        MessageDelivery delivery = deliveryOf(joinPoint, method);

        IdempotencyPolicy policy = PolicyResolver.resolve(annotation, properties);
        byte[] body = delivery.body();

        Optional<String> rawKey = annotation.header().isEmpty()
                ? BodyFieldKeyStrategy.resolve(body, annotation.fieldPath())
                : delivery.resolveHeaderKey(annotation.header());
        if (rawKey.isEmpty()) {
            if (policy.keyRequired()) {
                return delivery.deadLetter("key missing but required", "n/a");
            }
            // keyRequired=false: protection is a client opt-in - pass through unprotected, nothing cached.
            return joinPoint.proceed();
        }
        if (!KeyFormat.isValid(rawKey.get(), properties.getKey().getMaxLength())) {
            return delivery.deadLetter("key value invalid (size/charset)", rawKey.get());
        }

        EffectiveKey key = MessagingEffectiveKeyFactory.create(delivery.destination(), delivery.listenerId(), rawKey.get());
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
            log.debug("Duplicate delivery acked without re-invoking the listener: destination={} listener={} key={}",
                    key.route(), key.handler(), key.value());
            return null;
        }
        if (decision instanceof EngineDecision.Collision) {
            return delivery.deadLetter("collision (fingerprint mismatch)", key.value());
        }
        if (decision instanceof EngineDecision.Reject reject && reject.reason() == RejectReason.IN_PROGRESS) {
            log.debug("Concurrent duplicate delivery acked without invoking the listener: destination={} listener={} key={}",
                    key.route(), key.handler(), key.value());
            return null;
        }
        if (decision instanceof EngineDecision.FailClosed) {
            return delivery.failClosed();
        }
        throw new IllegalStateException("Idempotency decision " + decision + " is not yet handled for destination="
                + delivery.destination() + " listener=" + delivery.listenerId());
    }
}
