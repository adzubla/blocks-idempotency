package io.adzubla.blocks.idempotency.messaging.jms;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.postgres.PostgresIdempotencyStore;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.artemis.autoconfigure.ArtemisConfigurationCustomizer;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import jakarta.jms.ConnectionFactory;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end coverage of {@code @Idempotent(store = PostgresIdempotencyStore.QUALIFIER)}
 * over a real (embedded, in-VM ActiveMQ Artemis) broker and a real
 * Testcontainers Postgres (Slice 053): the same AOP advice + engine driving
 * {@link PostgresIdempotencyStore}, proving the PRD §7 claim that a
 * synchronous, single-session-thread {@code @JmsListener} satisfies the same
 * thread-bound transaction assumption a synchronous MVC controller does
 * (ADR 0003) - mirrors {@code KafkaPostgresIdempotencyEndToEndTest}/{@code
 * RabbitPostgresIdempotencyEndToEndTest}, now driven by a JMS listener
 * instead.
 *
 * <p>Where Kafka bounds a failing redelivery via {@code
 * DefaultErrorHandler(FixedBackOff)} on the container factory and RabbitMQ
 * uses a {@code RetryOperationsInterceptor}, plain JMS has neither - the
 * broker's own redelivery policy is the equivalent lever here: an {@link
 * ArtemisConfigurationCustomizer} bounds {@code orders-flaky}/{@code
 * orders-no-transactional}'s redelivery attempts so those tests' retries
 * don't run forever, and disables redelivery entirely on {@code
 * orders-concurrent} so a thrown exception there is final (same reasoning as
 * the other two brokers' dedicated concurrency-test container factory).
 */
@Testcontainers
@SpringBootTest(classes = JmsPostgresIdempotencyEndToEndTest.TestApplication.class,
        properties = "spring.artemis.mode=embedded")
class JmsPostgresIdempotencyEndToEndTest {

    private static final String HEADER = "IdempotencyKey";
    private static final String QUEUE = "orders";
    private static final String FLAKY_QUEUE = "orders-flaky";
    private static final String CONCURRENT_QUEUE = "orders-concurrent";
    private static final String NO_TRANSACTIONAL_QUEUE = "orders-no-transactional";

    @Container
    @ServiceConnection
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    private AtomicInteger listenerInvocations;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void resetFixtures() {
        listenerInvocations.set(0);
        jdbc.update("DELETE FROM test_orders");
        jdbc.update("DELETE FROM idempotency_record");
    }

    @Test
    void firstDeliveryExecutesTheListenerAndCompletesTheRecord() {
        send(QUEUE, "key-1", "{\"amount\":10}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
    }

    @Test
    void repeatDeliveryWithTheSameKeyIsAckedWithoutReinvokingTheListenerOrWritingAgain() {
        send(QUEUE, "key-2", "{\"amount\":10}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));

        send(QUEUE, "key-2", "{\"amount\":10}");

        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
    }

    /**
     * The listener's own {@code @Transactional} write shares the reservation's
     * transaction (ADR 0003): a thrown exception releases (rolls back) both
     * together, same as a non-2xx response does for HTTP. Each broker
     * redelivery re-invokes the AOP advice too, so it gets its own fresh
     * {@code reserve()} - exactly like a fresh Kafka/RabbitMQ redelivery would.
     * The assertions deliberately don't pin an exact invocation count - exactly
     * how many redeliveries the broker attempts before giving up isn't the
     * guarantee under test; the invariant that matters is that {@code
     * test_orders} never grows while every attempt keeps failing, and lands at
     * exactly one row once a genuinely successful delivery commits.
     */
    @Test
    void aThrownExceptionRollsBackTheReservationAndTheListenersOwnWriteTogether() {
        send(FLAKY_QUEUE, "key-flaky-1", "{\"mode\":\"throw\"}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isGreaterThanOrEqualTo(1));
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isZero());

        send(FLAKY_QUEUE, "key-flaky-2", "{\"mode\":\"ok\"}");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1));
    }

    /**
     * Two genuinely concurrent deliveries of the same idempotency key,
     * consumed by two different listener container session threads ({@code
     * concurrency = "2"} on {@code concurrentTestContainerFactory} - competing
     * consumers on the same queue) - proving Postgres's native concurrency (a
     * second {@code reserve()} blocks on the reservation row's own lock, ADR
     * 0001/0002/Slice 018) holds through the full JMS stack, not just the
     * store in isolation or the HTTP stack.
     */
    @Test
    void twoConcurrentDeliveriesOfTheSameKeyExecuteTheListenerExactlyOnce() {
        send(CONCURRENT_QUEUE, "concurrent-key", "{\"amount\":10}");
        send(CONCURRENT_QUEUE, "concurrent-key", "{\"amount\":10}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
    }

    /**
     * When the primary of two concurrent deliveries throws, the blocked
     * partner self-promotes to owner (ADR 0001/0002's "resolves according to
     * what the primary does with its transaction") and executes the effect
     * itself - proving "exactly once", not "zero or twice". {@code
     * concurrentTestContainerFactory}'s queue has redelivery disabled entirely
     * (see the {@code ArtemisConfigurationCustomizer} bean) - a thrown
     * exception there is final, so the only two invocations are the primary's
     * (failed) attempt and the waiter's self-promoted one.
     */
    @Test
    void whenThePrimaryThrowsTheBlockedConcurrentDeliverySelfPromotesAndExecutesTheListenerExactlyOnce() {
        send(CONCURRENT_QUEUE, "concurrent-key-throw", "{\"mode\":\"throw\"}");

        // Waiting for the primary to have won the reservation and entered its
        // sleep (listenerInvocations increments before the sleep, in onConcurrent)
        // before sending the waiter forces it to be the one left blocking on the
        // still-open transaction, rather than leaving "who becomes primary" to a
        // race between the two consumer sessions picking up messages off the queue.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));

        send(CONCURRENT_QUEUE, "concurrent-key-throw", "{\"mode\":\"ok\"}");

        // The effect itself (the row) is what must land exactly once - not
        // zero, not twice - regardless of exactly how the two invocations
        // (primary's failed attempt + the self-promoted waiter's) interleave.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(
                () -> assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1));
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(2));
    }

    /**
     * Mirrors {@code PostgresIdempotencyEndToEndTest
     * .aHandlerWithNoTransactionalAnnotationStillSharesTheReservationsTransaction}:
     * no {@code @Transactional} on the listener at all - Spring's
     * thread-bound resource binding (ADR 0003) means the plain {@code
     * JdbcTemplate} write still joins the already-open reservation
     * transaction, and a thrown exception still rolls both back together.
     */
    @Test
    void aThrownExceptionInAListenerWithNoTransactionalAnnotationStillRollsBackTogetherWithTheReservation() {
        send(NO_TRANSACTIONAL_QUEUE, "key-no-tx-1", "{\"mode\":\"error\"}");

        // See aThrownExceptionRollsBackTheReservationAndTheListenersOwnWriteTogether's
        // javadoc for why this doesn't pin an exact invocation count.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isGreaterThanOrEqualTo(1));
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isZero());

        send(NO_TRANSACTIONAL_QUEUE, "key-no-tx-2", "{\"mode\":\"ok\"}");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1));
    }

    private void send(String queue, String idempotencyKey, String payload) {
        jmsTemplate.convertAndSend(queue, payload, message -> {
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
        OrdersListener ordersListener(JdbcTemplate jdbc, AtomicInteger listenerInvocations) {
            return new OrdersListener(jdbc, listenerInvocations);
        }

        /**
         * No {@code sessionTransacted} setting needed: on the default
         * (non-JMS-transacted, {@code AUTO_ACKNOWLEDGE}) session Spring uses
         * here, a thrown listener exception still leaves the delivery
         * un-acked and triggers the broker's own redelivery via {@code
         * Session.recover()} - the same mechanism {@code
         * JmsIdempotencyAdvice}'s fail-closed seam relies on - so {@code
         * testRedeliveryPolicyCustomizer}'s per-queue {@code
         * AddressSettings} still govern how many times that redelivery is
         * retried.
         */
        @Bean
        DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ConnectionFactory connectionFactory) {
            DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
            factory.setConnectionFactory(new CachingConnectionFactory(connectionFactory));
            return factory;
        }

        /**
         * Dedicated factory for the "orders-concurrent" queue only: two
         * competing consumer sessions (independent of the default factory's
         * single consumer) so the same-key deliveries in the concurrency tests
         * race genuinely concurrently. Redelivery on this queue is disabled
         * entirely (see {@code testRedeliveryPolicyCustomizer}'s sibling bean
         * below) - a thrown exception there is final, exactly like an HTTP
         * handler's thrown exception is final with no automatic retry, so the
         * self-promotion test's outcome isn't complicated by a stray
         * redelivery racing back against the already-self-promoted waiter's
         * commit. Redelivery-driven rollback is proven separately, on the
         * default factory above.
         */
        @Bean
        DefaultJmsListenerContainerFactory concurrentTestContainerFactory(ConnectionFactory connectionFactory) {
            DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
            factory.setConnectionFactory(new CachingConnectionFactory(connectionFactory));
            factory.setConcurrency("2");
            return factory;
        }

        /**
         * Plain JMS has neither Kafka's {@code DefaultErrorHandler(FixedBackOff)}
         * nor RabbitMQ's {@code RetryOperationsInterceptor} - the broker's own
         * redelivery policy is the equivalent lever: bounds {@code
         * orders-flaky}/{@code orders-no-transactional}'s redelivery attempts
         * so those tests' retries don't run forever, and disables redelivery
         * entirely on {@code orders-concurrent} (see {@code
         * concurrentTestContainerFactory}'s javadoc).
         */
        @Bean
        ArtemisConfigurationCustomizer testRedeliveryPolicyCustomizer() {
            return configuration -> {
                configuration.addAddressSetting(FLAKY_QUEUE, new AddressSettings().setMaxDeliveryAttempts(3).setRedeliveryDelay(100L));
                configuration.addAddressSetting(NO_TRANSACTIONAL_QUEUE,
                        new AddressSettings().setMaxDeliveryAttempts(3).setRedeliveryDelay(100L));
                configuration.addAddressSetting(CONCURRENT_QUEUE, new AddressSettings().setMaxDeliveryAttempts(1));
            };
        }
    }

    static class OrdersListener {

        private final JdbcTemplate jdbc;
        private final AtomicInteger listenerInvocations;

        OrdersListener(JdbcTemplate jdbc, AtomicInteger listenerInvocations) {
            this.jdbc = jdbc;
            this.listenerInvocations = listenerInvocations;
        }

        @Idempotent(header = HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @JmsListener(id = "orders-listener", destination = QUEUE)
        @Transactional
        void onOrder(Message message) {
            listenerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 10);
        }

        @Idempotent(header = HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @JmsListener(id = "orders-flaky-listener", destination = FLAKY_QUEUE)
        @Transactional
        void onFlaky(Message message) throws Exception {
            listenerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 999);
            if (bodyOf(message).contains("throw")) {
                throw new IllegalStateException("boom");
            }
        }

        /**
         * Sleeps mid-listener so the concurrent partner delivery has a wide
         * window to observe {@code reserve()} blocking on the reservation
         * row's lock (Slice 018), rather than the race being too tight to
         * reliably exercise in a test.
         */
        @Idempotent(header = HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @JmsListener(id = "orders-concurrent-listener", destination = CONCURRENT_QUEUE, containerFactory = "concurrentTestContainerFactory")
        @Transactional
        void onConcurrent(Message message) throws Exception {
            listenerInvocations.incrementAndGet();
            Thread.sleep(300);
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 1);
            if (bodyOf(message).contains("throw")) {
                throw new IllegalStateException("boom");
            }
        }

        /** No {@code @Transactional} at all - see {@code aThrownExceptionInAListenerWithNoTransactionalAnnotationStillRollsBackTogetherWithTheReservation}. */
        @Idempotent(header = HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @JmsListener(id = "orders-no-transactional-listener", destination = NO_TRANSACTIONAL_QUEUE)
        void onNoTransactional(Message message) throws Exception {
            listenerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 555);
            if (bodyOf(message).contains("error")) {
                throw new IllegalStateException("boom");
            }
        }

        private static String bodyOf(Message message) throws Exception {
            return ((TextMessage) message).getText();
        }
    }
}
