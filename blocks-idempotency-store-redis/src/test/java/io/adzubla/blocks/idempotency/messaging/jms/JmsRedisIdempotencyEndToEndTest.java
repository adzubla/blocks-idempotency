package io.adzubla.blocks.idempotency.messaging.jms;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.redis.RedisIdempotencyStore;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Re-verifies the mechanics proven in Slices 049/050 against the real {@link
 * RedisIdempotencyStore} instead of {@code InMemoryIdempotencyStore}: the
 * same AOP advice + engine, over a real (embedded, in-VM ActiveMQ Artemis)
 * broker and a real (Testcontainers) Redis - proving the PRD's claim that
 * Redis needs no changes for messaging, since it already keys opaquely off
 * {@code EffectiveKey.digestBytes()}. No store-level code changes were
 * needed to make this pass.
 */
@Testcontainers
@SpringBootTest(classes = JmsRedisIdempotencyEndToEndTest.TestApplication.class,
        properties = "spring.artemis.mode=embedded")
class JmsRedisIdempotencyEndToEndTest {

    private static final String HEADER = "IdempotencyKey";
    private static final String QUEUE = "orders";

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getRedisHost);
        registry.add("spring.data.redis.port", REDIS::getRedisPort);
    }

    @Autowired
    private JmsTemplate jmsTemplate;

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

        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
    }

    @Test
    void differentKeysAreIsolated() {
        send("key-3", "{\"amount\":10}");
        send("key-4", "{\"amount\":20}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(2));
    }

    @Test
    void collisionWithADifferentBodyIsRoutedToTheDeadLetterDestinationInsteadOfInvokingTheListener() {
        send("key-collision", "{\"amount\":10}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));

        send("key-collision", "{\"amount\":20}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            TextMessage deadLettered = (TextMessage) jmsTemplate.receive(QUEUE + ".DLQ");
            assertThat(deadLettered).isNotNull();
            assertThat(deadLettered.getText()).isEqualTo("{\"amount\":20}");
            assertThat(deadLettered.getStringProperty(HEADER)).isEqualTo("key-collision");
        });

        // The original delivery's own completion is unaffected - the listener never ran a second time.
        assertThat(listenerInvocations.get()).isEqualTo(1);
    }

    @Test
    void missingKeyIsRoutedToTheDeadLetterDestinationInsteadOfInvokingTheListener() {
        jmsTemplate.convertAndSend(QUEUE, "{\"amount\":missing-key-marker}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            TextMessage deadLettered = (TextMessage) jmsTemplate.receive(QUEUE + ".DLQ");
            assertThat(deadLettered).isNotNull();
            assertThat(deadLettered.getText()).isEqualTo("{\"amount\":missing-key-marker}");
        });

        assertThat(listenerInvocations.get()).isZero();
    }

    private void send(String idempotencyKey, String body) {
        jmsTemplate.convertAndSend(QUEUE, body, message -> {
            message.setStringProperty(HEADER, idempotencyKey);
            return message;
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableJms
    static class TestApplication {

        @Bean
        AtomicInteger listenerInvocations() {
            return new AtomicInteger();
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

        @Idempotent(header = HEADER, store = RedisIdempotencyStore.QUALIFIER)
        @JmsListener(id = "orders-listener", destination = QUEUE)
        void onMessage(Message message) {
            listenerInvocations.incrementAndGet();
        }
    }
}
