package io.adzubla.blocks.idempotency.messaging.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Kafka-transport-specific idempotency settings. */
@ConfigurationProperties(prefix = "idempotency.kafka")
public class IdempotencyKafkaProperties {

    /** Suffix appended to a delivery's source topic to form its dead-letter topic. */
    private String deadLetterSuffix = ".DLT";

    public String getDeadLetterSuffix() {
        return deadLetterSuffix;
    }

    public void setDeadLetterSuffix(String deadLetterSuffix) {
        this.deadLetterSuffix = deadLetterSuffix;
    }
}
