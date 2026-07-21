package io.adzubla.blocks.idempotency.messaging.kafka;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
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
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end coverage of Slice 035: an {@code @Idempotent} + {@code
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
@EmbeddedKafka(partitions = 1, topics = {"orders"})
class KafkaIdempotencyEndToEndTest {

    private static final String HEADER = Idempotent.IDEMPOTENCY_KEY_HEADER;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @BeforeEach
    void resetFixtures() {
        listenerInvocations.set(0);
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

    private void send(String idempotencyKey, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>("orders", null, "record-key", payload);
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
}
