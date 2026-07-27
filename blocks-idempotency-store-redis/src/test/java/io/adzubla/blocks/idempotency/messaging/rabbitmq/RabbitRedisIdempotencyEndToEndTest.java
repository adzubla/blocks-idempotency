package io.adzubla.blocks.idempotency.messaging.rabbitmq;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.redis.RedisIdempotencyStore;
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
 * Re-verifies the mechanics proven in Slices 043/044 against the real {@link
 * RedisIdempotencyStore} instead of {@code InMemoryIdempotencyStore} (Slice
 * 046): the same AOP advice + engine, over a real (Testcontainers) RabbitMQ
 * broker and a real (Testcontainers) Redis - proving the PRD's claim that
 * Redis needs no changes for messaging, since it already keys opaquely off
 * {@code EffectiveKey.digestBytes()}. No store-level code changes were
 * needed to make this pass.
 */
@Testcontainers
@SpringBootTest(classes = RabbitRedisIdempotencyEndToEndTest.TestApplication.class)
class RabbitRedisIdempotencyEndToEndTest {

    @Container
    @ServiceConnection
    static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:3.13-management-alpine");

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getRedisHost);
        registry.add("spring.data.redis.port", REDIS::getRedisPort);
    }

    private static final String HEADER = Idempotent.IDEMPOTENCY_KEY_HEADER;
    private static final String QUEUE = "orders";
    private static final String DEAD_LETTER_EXCHANGE = "orders.dlx";
    private static final String DEAD_LETTER_QUEUE = "orders.dlq";

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @BeforeEach
    void resetFixtures() {
        listenerInvocations.set(0);
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
    void missingKeyIsDeadLetteredInsteadOfInvokingTheListener() {
        Message message = org.springframework.amqp.core.MessageBuilder
                .withBody("{\"amount\":missing-key-marker}".getBytes(StandardCharsets.UTF_8)).build();
        rabbitTemplate.send(QUEUE, message);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Message deadLettered = rabbitTemplate.receive(DEAD_LETTER_QUEUE);
            assertThat(deadLettered).isNotNull();
            assertThat(new String(deadLettered.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"amount\":missing-key-marker}");
        });

        assertThat(listenerInvocations.get()).isZero();
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
        @RabbitListener(id = "orders-listener", queues = QUEUE)
        void onMessage(Message message) {
            listenerInvocations.incrementAndGet();
        }
    }
}
