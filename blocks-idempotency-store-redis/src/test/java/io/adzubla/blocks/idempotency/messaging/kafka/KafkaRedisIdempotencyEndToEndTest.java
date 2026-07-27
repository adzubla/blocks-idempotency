package io.adzubla.blocks.idempotency.messaging.kafka;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.redis.RedisIdempotencyStore;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Re-verifies the mechanics proven in Slices 035/036 against the real {@link
 * RedisIdempotencyStore} instead of {@code InMemoryIdempotencyStore} (Slice
 * 041): the same AOP advice + engine, over a real (embedded) Kafka broker and
 * a real (Testcontainers) Redis - proving the PRD's claim that Redis needs no
 * changes for messaging, since it already keys opaquely off {@code
 * EffectiveKey.digestBytes()}. No store-level code changes were needed to
 * make this pass.
 */
@Testcontainers
@SpringBootTest(classes = KafkaRedisIdempotencyEndToEndTest.TestApplication.class)
@EmbeddedKafka(partitions = 1, topics = {"orders", "orders.DLT"})
class KafkaRedisIdempotencyEndToEndTest {

    private static final String HEADER = Idempotent.IDEMPOTENCY_KEY_HEADER;

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getRedisHost);
        registry.add("spring.data.redis.port", REDIS::getRedisPort);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

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

        ConsumerRecord<String, String> deadLettered = awaitDeadLetteredRecord("dlt-test-collision", "{\"amount\":20}");
        assertThat(deadLettered.key()).isEqualTo("record-key");
        assertThat(deadLettered.headers().lastHeader(HEADER).value()).isEqualTo("key-collision".getBytes(StandardCharsets.UTF_8));

        // The original delivery's own completion is unaffected - the listener never ran a second time.
        assertThat(listenerInvocations.get()).isEqualTo(1);
    }

    @Test
    void missingKeyIsRoutedToTheDeadLetterTopicInsteadOfInvokingTheListener() {
        kafkaTemplate.send(new ProducerRecord<>("orders", null, "record-key", "{\"amount\":missing-key-marker}"));

        ConsumerRecord<String, String> deadLettered = awaitDeadLetteredRecord("dlt-test-missing-key", "{\"amount\":missing-key-marker}");
        assertThat(deadLettered.key()).isEqualTo("record-key");

        assertThat(listenerInvocations.get()).isZero();
    }

    private void send(String idempotencyKey, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>("orders", null, "record-key", payload);
        record.headers().add(HEADER, idempotencyKey.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);
    }

    /**
     * Reads {@code orders.DLT} for the record matching {@code expectedValue}. The
     * embedded broker (and its topics) is shared across every test method in this
     * class, so a fresh consumer group reading "earliest" sees every prior test's
     * dead-lettered records too - filtering by the test's own distinctive payload,
     * rather than asserting the topic holds exactly one record, keeps this robust
     * regardless of method execution order.
     */
    private ConsumerRecord<String, String> awaitDeadLetteredRecord(String groupId, String expectedValue) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafkaBroker, groupId, true);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (Consumer<String, String> dltConsumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dltConsumer, "orders.DLT");
            AtomicReference<ConsumerRecord<String, String>> found = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(dltConsumer, Duration.ofSeconds(1));
                records.records("orders.DLT").forEach(record -> {
                    if (expectedValue.equals(record.value())) {
                        found.set(record);
                    }
                });
                assertThat(found.get()).isNotNull();
            });
            return found.get();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableKafka
    static class TestApplication {

        @Bean
        AtomicInteger listenerInvocations() {
            return new AtomicInteger();
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
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "idempotency-redis-test");
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

        @Idempotent(header = HEADER, store = RedisIdempotencyStore.QUALIFIER)
        @KafkaListener(id = "orders-listener", topics = "orders")
        void onMessage(ConsumerRecord<String, String> record) {
            listenerInvocations.incrementAndGet();
        }
    }
}
