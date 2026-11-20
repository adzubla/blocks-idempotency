package io.adzubla.blocks.idempotency.messaging.core;

import java.util.Optional;

/**
 * The broker-specific seam {@link AbstractMessagingIdempotencyAdvice} drives
 * one intercepted delivery through. Everything the broker-neutral decision
 * skeleton needs to read from a delivery, plus the two terminal actions whose
 * <em>mechanism</em> is genuinely broker-specific ({@link #deadLetter} and {@link #failClosed}), live behind this interface;
 * the skeleton itself is transport-agnostic.
 *
 * <p>A fresh instance is built per intercepted invocation (see {@link
 * AbstractMessagingIdempotencyAdvice#deliveryOf}), so an implementation may
 * capture the raw {@code ConsumerRecord}/{@code Message} it wraps.
 */
public interface MessageDelivery {

    /**
     * The route the {@link io.adzubla.blocks.idempotency.model.EffectiveKey} is scoped by: a Kafka topic, a
     * RabbitMQ queue, or a JMS queue/topic name.
     */
    String destination();

    /**
     * The handler the key is scoped by: the {@code @KafkaListener}/{@code @RabbitListener}/{@code @JmsListener}
     * id, or {@code <declaring class>#<method>} when the annotation sets none (see {@link MessagingListenerId}).
     */
    String listenerId();

    /**
     * The raw message payload, used both as the body-field key source and as the fingerprint input.
     * RabbitMQ always has raw bytes. Kafka accepts a {@code byte[]} or {@code String} value (a null
     * tombstone is an empty body), and JMS a {@code TextMessage} or {@code BytesMessage}.
     *
     * @throws UnsupportedMessagePayloadException if the payload isn't available as raw bytes or text
     *         (Kafka: an already-deserialized value; JMS: any other message type)
     */
    byte[] body();

    /** Resolves the raw key from the named broker header, absent if missing or blank (broker-specific header shape). */
    Optional<String> resolveHeaderKey(String headerName);

    /**
     * Terminal action for a structurally bad or poison delivery (missing/invalid key, or a
     * fingerprint collision) that no consumer-side retry can resolve. Kafka and JMS republish
     * to a dead-letter topic/destination and return {@code null} (acked, skipped); RabbitMQ
     * throws {@code org.springframework.amqp.AmqpRejectAndDontRequeueException} so the broker
     * dead-letters it. Any returned value is propagated as the advice result.
     *
     * @param reason short human-readable cause, for logging and (RabbitMQ) the rejection message
     * @param value  the offending key value, or {@code "n/a"} when no key was resolvable
     */
    Object deadLetter(String reason, String value) throws Throwable;

    /**
     * Terminal action when the store is unavailable and the resolved posture is {@code
     * onStoreFailure=CLOSED}: transient infrastructure trouble, not a poison message, so the
     * listener is not invoked and the delivery is handed back to the broker. Kafka and JMS throw
     * {@link IllegalStateException} (message left un-acked, so the container retries it or JMS rolls
     * the session back); RabbitMQ throws {@code
     * org.springframework.amqp.AmqpRejectAndDontRequeueException} (rejected without requeue, for
     * the queue's own dead-letter/retry policy). Always throws — never returns normally.
     */
    Object failClosed() throws Throwable;
}
