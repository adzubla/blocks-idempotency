package io.adzubla.blocks.idempotency.messaging.kafka.key;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Header strategy: the effective key's raw value comes from a
 * producer-supplied Kafka record header (see {@code CONTEXT.md} — Key
 * Resolution Strategy; the {@code ConsumerRecord.headers()} equivalent of
 * {@code web.key.HeaderKeyStrategy}).
 */
public final class KafkaHeaderKeyStrategy {

    private KafkaHeaderKeyStrategy() {
    }

    /** Resolves the raw key from the last header named {@code headerName}, absent if missing or blank. */
    public static Optional<String> resolve(Headers headers, String headerName) {
        Header header = headers.lastHeader(headerName);
        if (header == null || header.value() == null) {
            return Optional.empty();
        }
        String value = new String(header.value(), StandardCharsets.UTF_8);
        return value.isBlank() ? Optional.empty() : Optional.of(value);
    }
}
