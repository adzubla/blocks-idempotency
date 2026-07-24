package io.adzubla.blocks.idempotency.messaging.jms.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** JMS-transport-specific idempotency settings. */
@ConfigurationProperties(prefix = "idempotency.jms")
public class IdempotencyJmsProperties {

    /** Suffix appended to a delivery's source destination to form its dead-letter destination. */
    private String deadLetterSuffix = ".DLQ";

    public String getDeadLetterSuffix() {
        return deadLetterSuffix;
    }

    public void setDeadLetterSuffix(String deadLetterSuffix) {
        this.deadLetterSuffix = deadLetterSuffix;
    }
}
