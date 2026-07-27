package io.adzubla.blocks.idempotency.messaging.rabbitmq.key;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * Header strategy: the effective key's raw value comes from a
 * producer-supplied AMQP message header (see {@code CONTEXT.md} — Key
 * Resolution Strategy; the {@code MessageProperties.getHeaders()} equivalent
 * of {@code web.key.HeaderKeyStrategy}).
 */
public final class RabbitHeaderKeyStrategy {

    private RabbitHeaderKeyStrategy() {
    }

    /** Resolves the raw key from the header named {@code headerName}, absent if missing or blank. */
    public static Optional<String> resolve(Map<String, Object> headers, String headerName) {
        Object value = headers.get(headerName);
        if (value == null) {
            return Optional.empty();
        }
        String stringValue = value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : value.toString();
        return stringValue.isBlank() ? Optional.empty() : Optional.of(stringValue);
    }
}
