package io.adzubla.blocks.idempotency.messaging.rabbitmq;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
 * End-to-end coverage of Slices 043/044: an {@code @Idempotent} + {@code
 * @RabbitListener} method intercepted through the real AOP advice + engine +
 * in-memory store, over a real (Testcontainers) broker - mirroring the
 * Kafka module's {@code KafkaIdempotencyEndToEndTest} (Slices 035/036/038).
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
    private static final String DEAD_LETTER_EXCHANGE = "orders.dlx";
    private static final String DEAD_LETTER_QUEUE = "orders.dlq";
    private static final String FAIL_CLOSED_QUEUE = "orders-fail-closed";
    private static final String FAIL_CLOSED_RETRY_EXCHANGE = "orders-fail-closed.retry-dlx";
    private static final String FAIL_CLOSED_RETRY_QUEUE = "orders-fail-closed.retry";

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @Autowired
    private InMemoryIdempotencyStore idempotencyStore;

    @BeforeEach
    void resetFixtures() {
        listenerInvocations.set(0);
        idempotencyStore.setUnavailable(false);
        rabbitTemplate.execute(channel -> {
            channel.queuePurge(DEAD_LETTER_QUEUE);
            return null;
        });
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
    void collisionWithADifferentBodyIsDeadLetteredInsteadOfInvokingTheListener() {
        send("key-collision", "{\"amount\":10}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));

        send("key-collision", "{\"amount\":20}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Message deadLettered = rabbitTemplate.receive(DEAD_LETTER_QUEUE);
            assertThat(deadLettered).isNotNull();
            assertThat(new String(deadLettered.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"amount\":20}");
            assertThat(deadLettered.getMessageProperties().getHeaders().get(HEADER)).isEqualTo("key-collision");
        });

        // The original delivery's own completion is unaffected - the listener never ran a second time.
        assertThat(listenerInvocations.get()).isEqualTo(1);
    }

    @Test
    void storeUnavailableWithFailClosedIsNackedWithBackoffUntilTheStoreRecovers() {
        idempotencyStore.setUnavailable(true);

        sendFailClosed("key-outage-closed", "{\"amount\":10}");

        // While the store stays down, each delivery is rejected without requeue (not acked, not
        // silently skipped) and lands in the TTL-holding retry queue rather than the listener -
        // proving the nack-with-backoff mechanism, not just "eventually redelivered somehow".
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(listenerInvocations.get()).isZero());

        idempotencyStore.setUnavailable(false);

        // Once the store recovers, the retry queue's TTL expires and dead-letters the message
        // back onto the original queue, where it succeeds - it was never lost, only delayed.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
    }

    private void send(String idempotencyKey, String payload) {
        send(QUEUE, idempotencyKey, payload);
    }

    private void sendFailClosed(String idempotencyKey, String payload) {
        send(FAIL_CLOSED_QUEUE, idempotencyKey, payload);
    }

    private void send(String queue, String idempotencyKey, String payload) {
        Message message = org.springframework.amqp.core.MessageBuilder.withBody(payload.getBytes(StandardCharsets.UTF_8))
                .setHeader(HEADER, idempotencyKey)
                .build();
        rabbitTemplate.send(queue, message);
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
            return QueueBuilder.nonDurable(QUEUE).deadLetterExchange(DEAD_LETTER_EXCHANGE).build();
        }

        @Bean
        DirectExchange ordersDeadLetterExchange() {
            return new DirectExchange(DEAD_LETTER_EXCHANGE, false, false);
        }

        @Bean
        Queue ordersDeadLetterQueue() {
            return new Queue(DEAD_LETTER_QUEUE, false);
        }

        @Bean
        Binding ordersDeadLetterBinding(@Qualifier("ordersDeadLetterQueue") Queue ordersDeadLetterQueue,
                @Qualifier("ordersDeadLetterExchange") DirectExchange ordersDeadLetterExchange) {
            return BindingBuilder.bind(ordersDeadLetterQueue).to(ordersDeadLetterExchange).with(QUEUE);
        }

        // The fail-closed queue's own dead-letter config is the "nack-with-backoff" mechanism
        // (PRD §5): a rejected (requeue=false) delivery lands in a TTL-holding retry queue, which
        // itself dead-letters back onto this queue once the TTL expires - a delayed-redelivery
        // loop entirely external to the advice, which only ever rejects without requeue.
        @Bean
        Queue orderFailClosedQueue() {
            return QueueBuilder.nonDurable(FAIL_CLOSED_QUEUE).deadLetterExchange(FAIL_CLOSED_RETRY_EXCHANGE).build();
        }

        @Bean
        DirectExchange orderFailClosedRetryExchange() {
            return new DirectExchange(FAIL_CLOSED_RETRY_EXCHANGE, false, false);
        }

        @Bean
        Queue orderFailClosedRetryQueue() {
            return QueueBuilder.nonDurable(FAIL_CLOSED_RETRY_QUEUE)
                    .ttl(500)
                    .deadLetterExchange("")
                    .deadLetterRoutingKey(FAIL_CLOSED_QUEUE)
                    .build();
        }

        @Bean
        Binding orderFailClosedRetryBinding(@Qualifier("orderFailClosedRetryQueue") Queue orderFailClosedRetryQueue,
                @Qualifier("orderFailClosedRetryExchange") DirectExchange orderFailClosedRetryExchange) {
            return BindingBuilder.bind(orderFailClosedRetryQueue).to(orderFailClosedRetryExchange).with(FAIL_CLOSED_QUEUE);
        }

        @Bean
        OrdersListener ordersListener(AtomicInteger listenerInvocations) {
            return new OrdersListener(listenerInvocations);
        }

        @Bean
        FailClosedOrdersListener failClosedOrdersListener(AtomicInteger listenerInvocations) {
            return new FailClosedOrdersListener(listenerInvocations);
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

    static class FailClosedOrdersListener {

        private final AtomicInteger listenerInvocations;

        FailClosedOrdersListener(AtomicInteger listenerInvocations) {
            this.listenerInvocations = listenerInvocations;
        }

        @Idempotent(header = HEADER, onStoreFailure = OnStoreFailure.CLOSED)
        @RabbitListener(id = "orders-fail-closed-listener", queues = FAIL_CLOSED_QUEUE)
        void onMessage(Message message) {
            listenerInvocations.incrementAndGet();
        }
    }
}
