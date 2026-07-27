package io.adzubla.blocks.idempotency.messaging.rabbitmq;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.postgres.PostgresIdempotencyStore;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end coverage of {@code @Idempotent(store = PostgresIdempotencyStore.QUALIFIER)}
 * over a real (Testcontainers) RabbitMQ broker and a real Testcontainers
 * Postgres (Slice 047): the same AOP advice + engine driving {@link
 * PostgresIdempotencyStore}, proving the PRD §7 claim that a synchronous,
 * single-container-thread {@code @RabbitListener} satisfies the same
 * thread-bound transaction assumption a synchronous MVC controller does
 * (ADR 0003) - mirrors {@code KafkaPostgresIdempotencyEndToEndTest} (Slice
 * 042), now driven by a RabbitMQ listener instead of a Kafka one.
 *
 * <p>Where Kafka bounds a failing redelivery via {@code
 * DefaultErrorHandler(FixedBackOff)} on the container factory, RabbitMQ has
 * no offset-based redelivery to bound the same way - a {@code
 * RetryOperationsInterceptor} plays the equivalent role here: it retries the
 * whole listener invocation (AOP advice included, so each attempt gets its
 * own fresh {@code reserve()}) in-process a bounded number of times before
 * handing the delivery to a {@link RejectAndDontRequeueRecoverer}, which
 * rejects it without requeue instead of retrying forever.
 */
@Testcontainers
@SpringBootTest(classes = RabbitPostgresIdempotencyEndToEndTest.TestApplication.class)
class RabbitPostgresIdempotencyEndToEndTest {

    private static final String HEADER = Idempotent.IDEMPOTENCY_KEY_HEADER;
    private static final String QUEUE = "orders";
    private static final String FLAKY_QUEUE = "orders-flaky";
    private static final String CONCURRENT_QUEUE = "orders-concurrent";
    private static final String NO_TRANSACTIONAL_QUEUE = "orders-no-transactional";

    @Container
    @ServiceConnection
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Container
    @ServiceConnection
    static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:3.13-management-alpine");

    @Autowired
    private RabbitTemplate rabbitTemplate;

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
     * together, same as a non-2xx response does for HTTP. Each in-process
     * retry (the {@code RetryOperationsInterceptor} on {@code
     * flakyContainerFactory}) re-invokes the AOP advice too, so it gets its
     * own fresh {@code reserve()} - exactly like a fresh Kafka redelivery
     * would. The assertions deliberately don't pin an exact invocation count
     * - exactly how many attempts run before the interceptor gives up isn't
     * the guarantee under test; the invariant that matters is that {@code
     * test_orders} never grows while every attempt keeps failing, and lands
     * at exactly one row once a genuinely successful delivery commits.
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
     * consumed by two different listener container threads ({@code
     * concurrentConsumers = 2} on {@code concurrentTestContainerFactory}) -
     * proving Postgres's native concurrency (a second {@code reserve()}
     * blocks on the reservation row's own lock, ADR 0001/0002/Slice 018)
     * holds through the full RabbitMQ stack, not just the store in isolation
     * or the HTTP stack.
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
     * concurrentTestContainerFactory} has no retry interceptor - a thrown
     * exception there is final (the container's default requeue would just
     * hand the same message to whichever consumer thread is free next, which
     * would complicate rather than clarify this specific race), so the only
     * two invocations are the primary's (failed) attempt and the waiter's
     * self-promoted one.
     */
    @Test
    void whenThePrimaryThrowsTheBlockedConcurrentDeliverySelfPromotesAndExecutesTheListenerExactlyOnce() {
        send(CONCURRENT_QUEUE, "concurrent-key-throw", "{\"mode\":\"throw\"}");

        // Waiting for the primary to have won the reservation and entered its
        // sleep (listenerInvocations increments before the sleep, in onConcurrent)
        // before sending the waiter forces it to be the one left blocking on the
        // still-open transaction, rather than leaving "who becomes primary" to a
        // race between the two consumer threads picking up messages off the queue.
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
        Message message = MessageBuilder.withBody(payload.getBytes(StandardCharsets.UTF_8)).setHeader(HEADER, idempotencyKey).build();
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
        Queue ordersQueue() {
            return new Queue(QUEUE, false);
        }

        @Bean
        Queue ordersFlakyQueue() {
            return new Queue(FLAKY_QUEUE, false);
        }

        @Bean
        Queue ordersConcurrentQueue() {
            return new Queue(CONCURRENT_QUEUE, false);
        }

        @Bean
        Queue ordersNoTransactionalQueue() {
            return new Queue(NO_TRANSACTIONAL_QUEUE, false);
        }

        @Bean
        OrdersListener ordersListener(JdbcTemplate jdbc, AtomicInteger listenerInvocations) {
            return new OrdersListener(jdbc, listenerInvocations);
        }

        /**
         * Bounds the "flaky"/"no-transactional" listeners' thrown-exception
         * redelivery so those tests' retries don't run forever: a stateless
         * {@code RetryOperationsInterceptor} retries the listener invocation
         * in-process (AOP advice included) a bounded number of times, then
         * hands the delivery to a {@link RejectAndDontRequeueRecoverer}
         * instead of retrying forever.
         */
        @Bean
        SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
            SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
            factory.setConnectionFactory(connectionFactory);
            MethodInterceptor retryInterceptor = RetryInterceptorBuilder.stateless()
                    .maxRetries(2)
                    .backOffOptions(100L, 1.0, 100L)
                    .recoverer(new RejectAndDontRequeueRecoverer())
                    .build();
            factory.setAdviceChain(retryInterceptor);
            return factory;
        }

        /**
         * Dedicated factory for the "orders-concurrent" queue only: two
         * concurrent consumers (independent of the default factory's single
         * consumer) so the same-key deliveries in the concurrency tests race
         * genuinely concurrently, with no retry interceptor at all - a thrown
         * exception there is final, exactly like an HTTP handler's thrown
         * exception is final with no automatic retry, so the self-promotion
         * test's outcome isn't complicated by a stray retry racing back
         * against the already-self-promoted waiter's commit. Redelivery-driven
         * rollback is proven separately, on the default factory above.
         */
        @Bean
        SimpleRabbitListenerContainerFactory concurrentTestContainerFactory(ConnectionFactory connectionFactory) {
            SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
            factory.setConnectionFactory(connectionFactory);
            factory.setConcurrentConsumers(2);
            return factory;
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
        @RabbitListener(id = "orders-listener", queues = QUEUE)
        @Transactional
        void onOrder(Message message) {
            listenerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 10);
        }

        @Idempotent(header = HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @RabbitListener(id = "orders-flaky-listener", queues = FLAKY_QUEUE)
        @Transactional
        void onFlaky(Message message) {
            listenerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 999);
            if (new String(message.getBody(), StandardCharsets.UTF_8).contains("throw")) {
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
        @RabbitListener(id = "orders-concurrent-listener", queues = CONCURRENT_QUEUE, containerFactory = "concurrentTestContainerFactory")
        @Transactional
        void onConcurrent(Message message) throws InterruptedException {
            listenerInvocations.incrementAndGet();
            Thread.sleep(300);
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 1);
            if (new String(message.getBody(), StandardCharsets.UTF_8).contains("throw")) {
                throw new IllegalStateException("boom");
            }
        }

        /** No {@code @Transactional} at all - see {@code aThrownExceptionInAListenerWithNoTransactionalAnnotationStillRollsBackTogetherWithTheReservation}. */
        @Idempotent(header = HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @RabbitListener(id = "orders-no-transactional-listener", queues = NO_TRANSACTIONAL_QUEUE)
        void onNoTransactional(Message message) {
            listenerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 555);
            if (new String(message.getBody(), StandardCharsets.UTF_8).contains("error")) {
                throw new IllegalStateException("boom");
            }
        }
    }
}
