package io.adzubla.blocks.idempotency.messaging.jms;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import jakarta.jms.Message;
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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end coverage of Slice 049: an {@code @Idempotent} + {@code
 * @JmsListener} method intercepted through the real AOP advice + engine +
 * in-memory store, over a real (embedded, in-VM ActiveMQ Artemis) broker -
 * mirroring the Kafka and RabbitMQ modules' own foundation end-to-end tests.
 *
 * <p>The broker, {@code ConnectionFactory}, {@code JmsTemplate}, and listener
 * container are all Spring Boot auto-configured ({@code spring.artemis.mode=
 * embedded} with {@code artemis-jakarta-server} on the test classpath); the
 * module itself only ever needs {@code IdempotencyEngineRegistry} + {@code
 * IdempotencyProperties}, both from {@code blocks-idempotency-core}.
 */
@SpringBootTest(classes = JmsIdempotencyEndToEndTest.TestApplication.class,
        properties = {"idempotency.default-store=" + InMemoryIdempotencyStore.QUALIFIER, "spring.artemis.mode=embedded"})
class JmsIdempotencyEndToEndTest {

    private static final String HEADER = "IdempotencyKey";
    private static final String QUEUE = "orders";

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @BeforeEach
    void resetFixtures() {
        listenerInvocations.set(0);
    }

    @Test
    void firstDeliveryExecutesTheListener() {
        send("key-1", "{\"amount\":10}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
    }

    @Test
    void repeatDeliveryWithTheSameKeyIsNotReinvoked() {
        send("key-2", "{\"amount\":10}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));

        send("key-2", "{\"amount\":10}");

        // The effect runs exactly once: the duplicate is acked without re-invoking the listener.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
    }

    @Test
    void differentKeysAreEachInvoked() {
        send("key-3", "{\"amount\":10}");
        send("key-4", "{\"amount\":20}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(2));
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

        @Bean(InMemoryIdempotencyStore.QUALIFIER)
        InMemoryIdempotencyStore idempotencyStore() {
            return new InMemoryIdempotencyStore();
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
        @JmsListener(id = "orders-listener", destination = QUEUE)
        void onMessage(Message message) {
            listenerInvocations.incrementAndGet();
        }
    }
}
