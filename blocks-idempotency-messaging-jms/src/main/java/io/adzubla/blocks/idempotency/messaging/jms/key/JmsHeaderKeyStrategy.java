package io.adzubla.blocks.idempotency.messaging.jms.key;

import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;

import java.util.Optional;

/**
 * Header strategy: the effective key's raw value comes from a
 * producer-supplied JMS message string property (see {@code CONTEXT.md} — Key
 * Resolution Strategy; the {@code Message.getStringProperty(...)} equivalent
 * of {@code web.key.HeaderKeyStrategy} and the Kafka/RabbitMQ modules' own
 * header strategies).
 *
 * <p>JMS property names must obey the message-selector identifier syntax (JMS
 * spec §3.5.1) - Java-identifier-like, no hyphens - so an application using
 * this strategy sets {@code header} on {@code @Idempotent} to a JMS-valid
 * property name (e.g. {@code IdempotencyKey}) rather than the hyphenated HTTP
 * {@code X-Idempotency-Key} default.
 */
public final class JmsHeaderKeyStrategy {

    private JmsHeaderKeyStrategy() {
    }

    /** Resolves the raw key from the string property named {@code propertyName}, absent if missing or blank. */
    public static Optional<String> resolve(Message message, String propertyName) {
        String value;
        try {
            value = message.getStringProperty(propertyName);
        } catch (JMSException e) {
            throw new JMSRuntimeException("Failed to read idempotency key property '" + propertyName + "'", e.getErrorCode(), e);
        }
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
    }
}
