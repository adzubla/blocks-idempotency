package io.adzubla.blocks.idempotency.messaging.jms;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.artemis.autoconfigure.ArtemisConfigurationCustomizer;
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
 * End-to-end coverage of Slices 049/050: an {@code @Idempotent} + {@code
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
    private static final String FAIL_CLOSED_QUEUE = "orders-fail-closed";

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @Autowired
    private InMemoryIdempotencyStore idempotencyStore;

    @BeforeEach
    void resetFixtures() {
        listenerInvocations.set(0);
        idempotencyStore.setUnavailable(false);
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
    void storeUnavailableWithFailClosedLeavesTheMessageUnackedUntilTheStoreRecovers() {
        idempotencyStore.setUnavailable(true);

        sendTo(FAIL_CLOSED_QUEUE, "key-outage-closed", "{\"amount\":10}");

        // While the store stays down, the delivery keeps failing (thrown, not acked) rather
        // than being silently skipped - the listener is never invoked, and the broker's own
        // redelivery keeps retrying instead of giving up.
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(listenerInvocations.get()).isZero());

        idempotencyStore.setUnavailable(false);

        // Once the store recovers, the broker's redelivery lets this same message succeed -
        // it was never lost, only left un-acked.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
    }

    private void send(String idempotencyKey, String body) {
        sendTo(QUEUE, idempotencyKey, body);
    }

    private void sendTo(String queue, String idempotencyKey, String body) {
        jmsTemplate.convertAndSend(queue, body, message -> {
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

        @Bean
        FailClosedOrdersListener failClosedOrdersListener(AtomicInteger listenerInvocations) {
            return new FailClosedOrdersListener(listenerInvocations);
        }

        /**
         * The embedded broker's default redelivery policy dead-letters a message after a handful
         * of attempts - fine for a real deployment sized to its own outage window, but too fast for
         * this test's simulated outage. Retry indefinitely with a short fixed delay instead, the
         * same technique the Kafka module's own fail-closed end-to-end test uses (a {@code
         * FixedBackOff.UNLIMITED_ATTEMPTS} error handler).
         */
        @Bean
        ArtemisConfigurationCustomizer unlimitedRedeliveryCustomizer() {
            return configuration -> configuration.addAddressSetting("#",
                    new AddressSettings().setMaxDeliveryAttempts(-1).setRedeliveryDelay(200L));
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

    static class FailClosedOrdersListener {

        private final AtomicInteger listenerInvocations;

        FailClosedOrdersListener(AtomicInteger listenerInvocations) {
            this.listenerInvocations = listenerInvocations;
        }

        @Idempotent(header = HEADER, onStoreFailure = OnStoreFailure.CLOSED)
        @JmsListener(id = "orders-fail-closed-listener", destination = FAIL_CLOSED_QUEUE)
        void onMessage(Message message) {
            listenerInvocations.incrementAndGet();
        }
    }
}
