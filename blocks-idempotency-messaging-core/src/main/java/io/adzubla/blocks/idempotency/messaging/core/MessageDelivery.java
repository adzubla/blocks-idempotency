package io.adzubla.blocks.idempotency.messaging.core;

import java.util.Optional;

/**
 * The broker-specific seam {@link AbstractMessagingIdempotencyAdvice} drives
 * one intercepted delivery through. Everything the broker-neutral decision
 * skeleton needs to read from a delivery, plus the two terminal actions whose
 * <em>mechanism</em> is genuinely broker-specific, live behind this interface;
 * the skeleton itself is transport-agnostic.
 *
 * <p>A fresh instance is built per intercepted invocation (see {@link
 * AbstractMessagingIdempotencyAdvice#deliveryOf}), so an implementation may
 * capture the raw {@code ConsumerRecord}/{@code Message} it wraps.
 */
public interface MessageDelivery {

    /** The route the {@link io.adzubla.blocks.idempotency.model.EffectiveKey} is scoped by: a Kafka topic or a RabbitMQ queue. */
    String destination();

    /** The handler the key is scoped by: the {@code @KafkaListener}/{@code @RabbitListener} id (or the method's fully-qualified name). */
    String listenerId();

    /** The raw message payload, used both as the body-field key source and as the fingerprint input. */
    byte[] body();

    /** Resolves the raw key from the named broker header, absent if missing or blank (broker-specific header shape). */
    Optional<String> resolveHeaderKey(String headerName);

    /**
     * Terminal action for a structurally bad or poison delivery (missing/invalid key, or a
     * fingerprint collision) that no consumer-side retry can resolve. Kafka republishes to its
     * dead-letter topic and returns {@code null} (acked, skipped); RabbitMQ throws {@link
     * org.springframework.amqp.AmqpRejectAndDontRequeueException} so the broker dead-letters it.
     * Any returned value is propagated as the advice result.
     *
     * @param reason short human-readable cause, for logging and (RabbitMQ) the rejection message
     * @param value  the offending key value, or {@code "n/a"} when no key was resolvable
     */
    Object deadLetter(String reason, String value) throws Throwable;

    /**
     * Terminal action when the store is unavailable and the resolved posture is {@code
     * onStoreFailure=CLOSED}: transient infrastructure trouble, not a poison message, so the
     * listener is not invoked and the delivery is left for broker redelivery. Kafka throws {@link
     * IllegalStateException} (message left un-acked); RabbitMQ throws {@link
     * org.springframework.amqp.AmqpRejectAndDontRequeueException} (rejected without requeue, for
     * the queue's own retry/backoff policy). Always throws — never returns normally.
     */
    Object failClosed() throws Throwable;
}
