package io.adzubla.blocks.idempotency.messaging.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Publishes a delivery to its dead-letter topic (source topic + a configured
 * suffix, {@code idempotency.kafka.dead-letter-suffix}, default {@code
 * ".DLT"} - the same convention Spring Kafka's own {@code
 * DeadLetterPublishingRecoverer} defaults to) instead of invoking the
 * listener - a terminal case from the PRD §5 action-mapping table (currently
 * just collision, Slice 036; a future slice may route missing/invalid-key
 * deliveries through the same publisher) where no consumer-side retry can
 * resolve the delivery.
 *
 * <p>Holds the application's {@link KafkaTemplate} as a raw type
 * deliberately: an application's own template is typically parameterized to
 * its own key/value types (e.g. {@code KafkaTemplate<String, String>}), and
 * Spring's generic-aware injection only matches an exact parameterization -
 * a raw-typed dependency here lets this infrastructure bean bind to whatever
 * template the application already has, republishing the original
 * key/value/headers unchanged without needing to know their type.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class KafkaDeadLetterPublisher {

    private final KafkaTemplate kafkaTemplate;
    private final String deadLetterSuffix;

    public KafkaDeadLetterPublisher(KafkaTemplate kafkaTemplate, String deadLetterSuffix) {
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterSuffix = deadLetterSuffix;
    }

    /** Republishes {@code record}'s key/value/headers, unchanged, to its dead-letter topic. */
    public void publish(ConsumerRecord<?, ?> record) {
        if (kafkaTemplate == null) {
            throw new IllegalStateException("Idempotency dead-letter routing required for topic=" + record.topic()
                    + " but no KafkaTemplate bean is configured to publish to " + deadLetterTopic(record));
        }
        kafkaTemplate.send(new ProducerRecord(deadLetterTopic(record), null, record.key(), record.value(), record.headers()));
    }

    private String deadLetterTopic(ConsumerRecord<?, ?> record) {
        return record.topic() + deadLetterSuffix;
    }
}
