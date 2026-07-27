package io.adzubla.blocks.idempotency.messaging.kafka;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.store.redis.RedisIdempotencyStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.backoff.FixedBackOff;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end proof that a genuine Redis outage (the container stopped, not
 * simulated) drives the existing {@code onStoreFailure} posture correctly
 * through the real Kafka AOP advice/engine/{@link RedisIdempotencyStore}
 * stack (Slice 041) - the posture logic itself is already covered generically
 * ({@code KafkaIdempotencyAdviceTest}, {@code InMemoryIdempotencyStore}); this
 * proves the wiring: that a real store failure reaches it as a {@code
 * StoreUnavailableException}, same reasoning as {@code
 * RedisIdempotencyOutageEndToEndTest} for HTTP.
 *
 * <p>A dedicated container, stopped mid-class and never restarted - this test
 * class's Redis is a one-way trip, same reasoning as that HTTP test.
 */
@Testcontainers
@SpringBootTest(classes = KafkaRedisIdempotencyOutageEndToEndTest.TestApplication.class)
@EmbeddedKafka(partitions = 1, topics = {"orders-open", "orders-closed"})
class KafkaRedisIdempotencyOutageEndToEndTest {

    private static final String HEADER = Idempotent.IDEMPOTENCY_KEY_HEADER;

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getRedisHost);
        registry.add("spring.data.redis.port", REDIS::getRedisPort);
        // Short command timeout: Lettuce's default (60s) would make every
        // delivery against the stopped container in this test take a minute.
        registry.add("spring.data.redis.timeout", () -> "2s");
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @Test
    void aStoppedRedisFailsOpenByDefaultAndLeavesTheDeliveryUnackedWhenFailClosed() {
        // Sanity check while Redis is still up: both listeners work normally.
        send("orders-open", "sanity-open", "{}");
        send("orders-closed", "sanity-closed", "{}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(2));

        REDIS.stop();

        // Default posture (fail-open): the listener still runs, unprotected, rather
        // than the outage silently swallowing every delivery.
        send("orders-open", "outage-open", "{}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(3));

        // onStoreFailure=CLOSED: the delivery is left un-acked (thrown, not invoked)
        // rather than running unprotected - repeatedly, since nothing acks it and the
        // container keeps redelivering; the listener is never invoked either way.
        send("orders-closed", "outage-closed", "{}");
        await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(3));
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
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "idempotency-redis-outage-test");
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
            // A thrown exception (onStoreFailure=CLOSED) must not exhaust a default retry
            // budget and be skipped anyway - retry indefinitely with a short, fixed delay.
            factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(200L, FixedBackOff.UNLIMITED_ATTEMPTS)));
            return factory;
        }
    }

    static class OrdersListener {

        private final AtomicInteger listenerInvocations;

        OrdersListener(AtomicInteger listenerInvocations) {
            this.listenerInvocations = listenerInvocations;
        }

        @Idempotent(header = HEADER, store = RedisIdempotencyStore.QUALIFIER)
        @KafkaListener(id = "orders-open-listener", topics = "orders-open")
        void onOpenMessage(ConsumerRecord<String, String> record) {
            listenerInvocations.incrementAndGet();
        }

        @Idempotent(header = HEADER, store = RedisIdempotencyStore.QUALIFIER, onStoreFailure = OnStoreFailure.CLOSED)
        @KafkaListener(id = "orders-closed-listener", topics = "orders-closed")
        void onClosedMessage(ConsumerRecord<String, String> record) {
            listenerInvocations.incrementAndGet();
        }
    }
}
