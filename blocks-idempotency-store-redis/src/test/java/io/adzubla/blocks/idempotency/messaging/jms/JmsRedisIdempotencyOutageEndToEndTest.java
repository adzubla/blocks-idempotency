package io.adzubla.blocks.idempotency.messaging.jms;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.store.redis.RedisIdempotencyStore;
import jakarta.jms.Message;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.artemis.autoconfigure.ArtemisConfigurationCustomizer;
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
 * End-to-end proof that a genuine Redis outage (the container stopped, not
 * simulated) drives the existing {@code onStoreFailure} posture correctly
 * through the real JMS AOP advice/engine/{@link RedisIdempotencyStore} stack
 * - the posture logic itself is already covered generically ({@code
 * JmsIdempotencyAdviceTest}, {@code InMemoryIdempotencyStore}); this proves
 * the wiring: that a real store failure reaches it as a {@code
 * StoreUnavailableException}, same reasoning as {@code
 * KafkaRedisIdempotencyOutageEndToEndTest}/{@code
 * RabbitRedisIdempotencyOutageEndToEndTest}.
 *
 * <p>A dedicated container, stopped mid-class and never restarted - this
 * test class's Redis is a one-way trip, same reasoning as those tests.
 */
@Testcontainers
@SpringBootTest(classes = JmsRedisIdempotencyOutageEndToEndTest.TestApplication.class,
        properties = "spring.artemis.mode=embedded")
class JmsRedisIdempotencyOutageEndToEndTest {

    private static final String HEADER = "IdempotencyKey";
    private static final String OPEN_QUEUE = "orders-open";
    private static final String CLOSED_QUEUE = "orders-closed";

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
    private JmsTemplate jmsTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @Test
    void aStoppedRedisFailsOpenByDefaultAndLeavesTheDeliveryUnackedWhenFailClosed() {
        // Sanity check while Redis is still up: both listeners work normally.
        send(OPEN_QUEUE, "sanity-open", "{}");
        send(CLOSED_QUEUE, "sanity-closed", "{}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(2));

        REDIS.stop();

        // Default posture (fail-open): the listener still runs, unprotected, rather
        // than the outage silently swallowing every delivery.
        send(OPEN_QUEUE, "outage-open", "{}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(3));

        // onStoreFailure=CLOSED: the delivery is left un-acked (thrown, not invoked)
        // rather than running unprotected - repeatedly, since nothing acks it and the
        // broker keeps redelivering; the listener is never invoked either way.
        send(CLOSED_QUEUE, "outage-closed", "{}");
        await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(3));
    }

    private void send(String queue, String idempotencyKey, String body) {
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

        @Bean
        OrdersListener ordersListener(AtomicInteger listenerInvocations) {
            return new OrdersListener(listenerInvocations);
        }

        /**
         * The embedded broker's default redelivery policy dead-letters a message
         * after a handful of attempts - fine for a real deployment sized to its own
         * outage window, but too fast for this test's one-way Redis outage. Retry
         * indefinitely with a short fixed delay instead, mirroring {@code
         * JmsIdempotencyEndToEndTest}'s own fail-closed coverage.
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

        @Idempotent(header = HEADER, store = RedisIdempotencyStore.QUALIFIER)
        @JmsListener(id = "orders-open-listener", destination = OPEN_QUEUE)
        void onOpenMessage(Message message) {
            listenerInvocations.incrementAndGet();
        }

        @Idempotent(header = HEADER, store = RedisIdempotencyStore.QUALIFIER, onStoreFailure = OnStoreFailure.CLOSED)
        @JmsListener(id = "orders-closed-listener", destination = CLOSED_QUEUE)
        void onClosedMessage(Message message) {
            listenerInvocations.incrementAndGet();
        }
    }
}
