package io.adzubla.blocks.idempotency.messaging.kafka.key;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaHeaderKeyStrategyTest {

    @Test
    void resolvesTheHeaderValueWhenPresent() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("Idempotency-Key", "key-1".getBytes(StandardCharsets.UTF_8));

        assertThat(KafkaHeaderKeyStrategy.resolve(headers, "Idempotency-Key")).contains("key-1");
    }

    @Test
    void absentWhenTheHeaderIsMissing() {
        RecordHeaders headers = new RecordHeaders();

        assertThat(KafkaHeaderKeyStrategy.resolve(headers, "Idempotency-Key")).isEqualTo(Optional.empty());
    }

    @Test
    void absentWhenTheHeaderValueIsBlank() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("Idempotency-Key", "   ".getBytes(StandardCharsets.UTF_8));

        assertThat(KafkaHeaderKeyStrategy.resolve(headers, "Idempotency-Key")).isEqualTo(Optional.empty());
    }

    @Test
    void resolvesTheLastValueWhenTheHeaderAppearsMoreThanOnce() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("Idempotency-Key", "key-old".getBytes(StandardCharsets.UTF_8));
        headers.add("Idempotency-Key", "key-new".getBytes(StandardCharsets.UTF_8));

        assertThat(KafkaHeaderKeyStrategy.resolve(headers, "Idempotency-Key")).contains("key-new");
    }
}
