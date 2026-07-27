package io.adzubla.blocks.idempotency.messaging.rabbitmq;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.store.redis.RedisIdempotencyStore;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end proof that a genuine Redis outage (the container stopped, not
 * simulated) drives the existing {@code onStoreFailure} posture correctly
 * through the real RabbitMQ AOP advice/engine/{@link RedisIdempotencyStore}
 * stack (Slice 046) - the posture logic itself is already covered
 * generically ({@code RabbitIdempotencyAdviceTest}, {@code
 * InMemoryIdempotencyStore}); this proves the wiring: that a real store
 * failure reaches it as a {@code StoreUnavailableException}, same reasoning
 * as {@code KafkaRedisIdempotencyOutageEndToEndTest}.
 *
 * <p>A dedicated container, stopped mid-class and never restarted - this
 * test class's Redis is a one-way trip, same reasoning as that test.
 */
@Testcontainers
@SpringBootTest(classes = RabbitRedisIdempotencyOutageEndToEndTest.TestApplication.class)
class RabbitRedisIdempotencyOutageEndToEndTest {

    private static final String HEADER = Idempotent.IDEMPOTENCY_KEY_HEADER;
    private static final String OPEN_QUEUE = "orders-open";
    private static final String CLOSED_QUEUE = "orders-closed";

    @Container
    @ServiceConnection
    static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:3.13-management-alpine");

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
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @Test
    void aStoppedRedisFailsOpenByDefaultAndRejectsTheDeliveryWithoutInvokingTheListenerWhenFailClosed() {
        // Sanity check while Redis is still up: both listeners work normally.
        send(OPEN_QUEUE, "sanity-open", "{}");
        send(CLOSED_QUEUE, "sanity-closed", "{}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(2));

        REDIS.stop();

        // Default posture (fail-open): the listener still runs, unprotected, rather
        // than the outage silently swallowing every delivery.
        send(OPEN_QUEUE, "outage-open", "{}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(3));

        // onStoreFailure=CLOSED: the delivery is rejected without requeue (the
        // FailClosed branch throws AmqpRejectAndDontRequeueException, Slice 044)
        // rather than running unprotected - the listener is never invoked, whether
        // or not the operator has configured a retry/backoff DLX on this queue.
        send(CLOSED_QUEUE, "outage-closed", "{}");
        await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(3));
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

        @Bean
        Queue ordersOpenQueue() {
            return new Queue(OPEN_QUEUE, false);
        }

        @Bean
        Queue ordersClosedQueue() {
            return new Queue(CLOSED_QUEUE, false);
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
        @RabbitListener(id = "orders-open-listener", queues = OPEN_QUEUE)
        void onOpenMessage(Message message) {
            listenerInvocations.incrementAndGet();
        }

        @Idempotent(header = HEADER, store = RedisIdempotencyStore.QUALIFIER, onStoreFailure = OnStoreFailure.CLOSED)
        @RabbitListener(id = "orders-closed-listener", queues = CLOSED_QUEUE)
        void onClosedMessage(Message message) {
            listenerInvocations.incrementAndGet();
        }
    }
}
