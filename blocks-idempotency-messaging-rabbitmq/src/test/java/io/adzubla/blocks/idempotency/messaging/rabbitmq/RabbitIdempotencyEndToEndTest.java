package io.adzubla.blocks.idempotency.messaging.rabbitmq;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end coverage of Slice 043: an {@code @Idempotent} + {@code
 * @RabbitListener} method intercepted through the real AOP advice + engine +
 * in-memory store, over a real (Testcontainers) broker - mirroring the
 * Kafka module's {@code KafkaIdempotencyEndToEndTest} (Slice 035).
 */
@Testcontainers
@SpringBootTest(classes = RabbitIdempotencyEndToEndTest.TestApplication.class,
        properties = "idempotency.default-store=" + InMemoryIdempotencyStore.QUALIFIER)
class RabbitIdempotencyEndToEndTest {

    @Container
    @ServiceConnection
    static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:3.13-management-alpine");

    private static final String HEADER = Idempotent.IDEMPOTENCY_KEY_HEADER;
    private static final String QUEUE = "orders";

    @Autowired
    private RabbitTemplate rabbitTemplate;

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
        Message message = org.springframework.amqp.core.MessageBuilder.withBody(payload.getBytes(StandardCharsets.UTF_8))
                .setHeader(HEADER, idempotencyKey)
                .build();
        rabbitTemplate.send(QUEUE, message);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
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
        Queue ordersQueue() {
            return new Queue(QUEUE, false);
        }

        @Bean
        OrdersListener ordersListener(AtomicInteger listenerInvocations) {
            return new OrdersListener(listenerInvocations);
        }
    }

    static class OrdersListener {

        private final AtomicInteger listenerInvocations;

        OrdersListener(AtomicInteger listenerInvocations) {
            this.listenerInvocations = listenerInvocations;
        }

        @Idempotent(header = HEADER)
        @RabbitListener(id = "orders-listener", queues = QUEUE)
        void onMessage(Message message) {
            listenerInvocations.incrementAndGet();
        }
    }
}
