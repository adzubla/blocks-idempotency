package io.adzubla.blocks.idempotency.messaging.kafka;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end coverage of Slices 035/036: an {@code @Idempotent} + {@code
 * @KafkaListener} method intercepted through the real AOP advice + engine +
 * in-memory store, over a real (embedded) broker.
 *
 * <p>Producer/consumer factories are wired by hand (no Spring Boot Kafka
 * auto-configuration on the classpath for this transport-neutral-core-only
 * module) - the module itself only ever needs {@code IdempotencyEngineRegistry}
 * + {@code IdempotencyProperties}, both from {@code blocks-idempotency-core}.
 */
@SpringBootTest(classes = KafkaIdempotencyEndToEndTest.TestApplication.class,
        properties = "idempotency.default-store=" + InMemoryIdempotencyStore.QUALIFIER)
@EmbeddedKafka(partitions = 1, topics = {"orders", "orders.DLT", "orders-fail-closed"})
class KafkaIdempotencyEndToEndTest {

    private static final String HEADER = Idempotent.IDEMPOTENCY_KEY_HEADER;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @Autowired
    private InMemoryIdempotencyStore idempotencyStore;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @BeforeEach
    void resetFixtures() {
        listenerInvocations.set(0);
        idempotencyStore.setUnavailable(false);
    }

    @Test
    void firstDeliveryExecutesTheListenerAndCompletesTheRecord() {
        send("key-1", "{\"amount\":10}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
    }

    @Test
    void repeatDeliveryWithTheSameKeyIsAckedWithoutReinvokingTheListener() {
        send("key-2", "{\"amount\":10}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));

        send("key-2", "{\"amount\":10}");

        // Give the (non-)invocation time to happen, then confirm the count never moved past 1.
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
    }

    @Test
    void differentKeysAreIsolated() {
        send("key-3", "{\"amount\":10}");
        send("key-4", "{\"amount\":10}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(2));
    }

    @Test
    void collisionWithADifferentBodyIsRoutedToTheDeadLetterTopicInsteadOfInvokingTheListener() {
        send("key-collision", "{\"amount\":10}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));

        send("key-collision", "{\"amount\":20}");

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafkaBroker, "dlt-test", true);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (Consumer<String, String> dltConsumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dltConsumer, "orders.DLT");
            ConsumerRecord<String, String> deadLettered = KafkaTestUtils.getSingleRecord(dltConsumer, "orders.DLT", Duration.ofSeconds(15));
            assertThat(deadLettered.key()).isEqualTo("record-key");
            assertThat(deadLettered.value()).isEqualTo("{\"amount\":20}");
            assertThat(deadLettered.headers().lastHeader(HEADER).value()).isEqualTo("key-collision".getBytes(StandardCharsets.UTF_8));
        }

        // The original delivery's own completion is unaffected - the listener never ran a second time.
        assertThat(listenerInvocations.get()).isEqualTo(1);
    }

    @Test
    void storeUnavailableWithFailClosedLeavesTheMessageUnackedUntilTheStoreRecovers() {
        idempotencyStore.setUnavailable(true);

        send("orders-fail-closed", "key-outage-closed", "{\"amount\":10}");

        // While the store stays down, the delivery keeps failing (thrown, not acked) rather
        // than being silently skipped - the listener is never invoked, and the container's
        // error handler (FixedBackOff, configured below) keeps retrying instead of giving up.
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(listenerInvocations.get()).isZero());

        idempotencyStore.setUnavailable(false);

        // Once the store recovers, the broker's redelivery (via the container's retry) lets
        // this same message succeed - it was never lost, only left un-acked.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
    }

    private void send(String idempotencyKey, String payload) {
        send("orders", idempotencyKey, payload);
    }

    private void send(String topic, String idempotencyKey, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, "record-key", payload);
        record.headers().add(HEADER, idempotencyKey.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableKafka
    static class TestApplication {

        @Bean
        AtomicInteger listenerInvocations() {
            return new AtomicInteger();
        }

        @Bean(InMemoryIdempotencyStore.QUALIFIER)
        InMemoryIdempotencyStore idempotencyStore() {
            return new InMemoryIdempotencyStore();
        }

        @Bean
        OrdersListener ordersListener(AtomicInteger listenerInvocations) {
            return new OrdersListener(listenerInvocations);
        }

        @Bean
        FailClosedOrdersListener failClosedOrdersListener(AtomicInteger listenerInvocations) {
            return new FailClosedOrdersListener(listenerInvocations);
        }

        @Bean
        ProducerFactory<String, String> producerFactory(@Value("${spring.embedded.kafka.brokers}") String brokers) {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(config);
        }

        @Bean
        KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }

        @Bean
        ConsumerFactory<String, String> consumerFactory(@Value("${spring.embedded.kafka.brokers}") String brokers) {
            Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "idempotency-test");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return new DefaultKafkaConsumerFactory<>(config);
        }

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
                ConsumerFactory<String, String> consumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            // A thrown exception (e.g. onStoreFailure=CLOSED, Slice 038) must not be lost to a
            // default retry budget that gives up and commits the offset anyway - retry
            // indefinitely with a short, fixed delay so the message survives until whatever
            // made the store unavailable resolves.
            factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(200L, FixedBackOff.UNLIMITED_ATTEMPTS)));
            return factory;
        }
    }

    static class OrdersListener {

        private final AtomicInteger listenerInvocations;

        OrdersListener(AtomicInteger listenerInvocations) {
            this.listenerInvocations = listenerInvocations;
        }

        @Idempotent(header = HEADER)
        @KafkaListener(id = "orders-listener", topics = "orders")
        void onMessage(ConsumerRecord<String, String> record) {
            listenerInvocations.incrementAndGet();
        }
    }

    static class FailClosedOrdersListener {

        private final AtomicInteger listenerInvocations;

        FailClosedOrdersListener(AtomicInteger listenerInvocations) {
            this.listenerInvocations = listenerInvocations;
        }

        @Idempotent(header = HEADER, onStoreFailure = OnStoreFailure.CLOSED)
        @KafkaListener(id = "orders-fail-closed-listener", topics = "orders-fail-closed")
        void onMessage(ConsumerRecord<String, String> record) {
            listenerInvocations.incrementAndGet();
        }
    }
}
