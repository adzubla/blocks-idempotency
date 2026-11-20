package io.adzubla.blocks.idempotency.messaging.core;

/**
 * Thrown from an {@code @Idempotent} message listener, without invoking it, when the delivery's
 * key is still {@code IN_PROGRESS}: another delivery holds the reservation, or a primary that
 * crashed or failed to release left a stale lock behind. The delivery is failed rather than
 * acked so the broker redelivers it. Once the key resolves, a redelivery either runs (the key was
 * released or its lock expired) or is acked as a duplicate (completed). Acking it here instead
 * could drop the only remaining copy of a message whose primary never finished (ADR 0005, amended).
 *
 * <p>Every supported container treats this as an ordinary, retryable listener failure: Kafka's
 * error handler retries it, RabbitMQ requeues it, and JMS rolls back a transacted session for redelivery.
 * How quickly it comes back is the container's own retry/backoff policy. Configure a backoff on
 * this type that comfortably exceeds {@code idempotency.lock-ttl}, so that a stale lock can expire
 * before the retries run out.
 */
public class DeliveryInProgressException extends RuntimeException {

    public DeliveryInProgressException(String message) {
        super(message);
    }
}
