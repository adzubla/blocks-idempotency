package io.adzubla.blocks.idempotency.messaging.core;

/**
 * Thrown from an {@code @Idempotent} message listener, without invoking it, when the delivery's
 * payload isn't available as raw bytes or text, for example a Kafka value already deserialized
 * to an object, or a JMS {@code ObjectMessage}/{@code MapMessage}. The fingerprint (and a {@code
 * fieldPath} key) is computed from the raw payload. Treating an unreadable payload as an empty
 * body would silently disable collision detection and send every {@code fieldPath} delivery to the
 * dead-letter destination. This is a configuration error: consume the raw payload instead.
 *
 * <p>The listener isn't invoked and the delivery isn't acked as a success. What happens to the
 * message next is the container's error handling: retried, then dead-lettered or logged and
 * skipped once retries run out. Since retrying can't fix a configuration error, consider
 * classifying this type as not retryable (e.g. Kafka's {@code
 * DefaultErrorHandler#addNotRetryableExceptions}) so it goes straight to your dead-letter path.
 */
public class UnsupportedMessagePayloadException extends RuntimeException {

    public UnsupportedMessagePayloadException(String message) {
        super(message);
    }
}
