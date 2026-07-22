package io.adzubla.blocks.idempotency.messaging.rabbitmq.key;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitHeaderKeyStrategyTest {

    @Test
    void resolvesTheHeaderValueWhenPresent() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("Idempotency-Key", "key-1");

        assertThat(RabbitHeaderKeyStrategy.resolve(headers, "Idempotency-Key")).contains("key-1");
    }

    @Test
    void absentWhenTheHeaderIsMissing() {
        Map<String, Object> headers = new HashMap<>();

        assertThat(RabbitHeaderKeyStrategy.resolve(headers, "Idempotency-Key")).isEqualTo(Optional.empty());
    }

    @Test
    void absentWhenTheHeaderValueIsBlank() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("Idempotency-Key", "   ");

        assertThat(RabbitHeaderKeyStrategy.resolve(headers, "Idempotency-Key")).isEqualTo(Optional.empty());
    }

    @Test
    void resolvesAByteArrayHeaderValueAsUtf8() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("Idempotency-Key", "key-bytes".getBytes(StandardCharsets.UTF_8));

        assertThat(RabbitHeaderKeyStrategy.resolve(headers, "Idempotency-Key")).contains("key-bytes");
    }
}
