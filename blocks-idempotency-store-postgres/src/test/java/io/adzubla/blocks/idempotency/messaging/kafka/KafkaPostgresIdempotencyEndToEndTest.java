package io.adzubla.blocks.idempotency.messaging.kafka;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.postgres.PostgresIdempotencyStore;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.backoff.FixedBackOff;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end coverage of {@code @Idempotent(store = PostgresIdempotencyStore.QUALIFIER)}
 * over a real (embedded) Kafka broker and a real Testcontainers Postgres
 * (Slice 042): the same AOP advice + engine driving {@link
 * PostgresIdempotencyStore}, proving the PRD §7 claim that a synchronous,
 * single-container-thread {@code @KafkaListener} satisfies the same
 * thread-bound transaction assumption a synchronous MVC controller does
 * (ADR 0003) - mirrors {@code PostgresIdempotencyEndToEndTest} (HTTP), now
 * driven by a listener instead of a controller.
 */
@Testcontainers
@SpringBootTest(classes = KafkaPostgresIdempotencyEndToEndTest.TestApplication.class)
@EmbeddedKafka(partitions = 2, topics = {"orders", "orders-flaky", "orders-concurrent", "orders-no-transactional"})
class KafkaPostgresIdempotencyEndToEndTest {

    private static final String HEADER = Idempotent.IDEMPOTENCY_KEY_HEADER;

    @Container
    @ServiceConnection
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

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
        send("orders", "key-1", "{\"amount\":10}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
    }

    @Test
    void repeatDeliveryWithTheSameKeyIsAckedWithoutReinvokingTheListenerOrWritingAgain() {
        send("orders", "key-2", "{\"amount\":10}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));

        send("orders", "key-2", "{\"amount\":10}");

        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
    }

    /**
     * The listener's own {@code @Transactional} write shares the reservation's
     * transaction (ADR 0003): a thrown exception releases (rolls back) both
     * together, same as a non-2xx response does for HTTP. Kafka has no
     * response status to key off - a thrown exception is the only "failure"
     * signal here, and the bounded {@link FixedBackOff} on {@code
     * kafkaListenerContainerFactory} keeps the resulting redeliveries from
     * retrying forever. The assertions deliberately don't pin an exact
     * invocation count - exactly how many times Kafka redelivers a failing
     * record before giving up isn't the guarantee under test (and is itself
     * timing-sensitive under load); the invariant that matters is that
     * {@code test_orders} never grows while every attempt keeps failing, and
     * lands at exactly one row once a genuinely successful delivery commits.
     */
    @Test
    void aThrownExceptionRollsBackTheReservationAndTheListenersOwnWriteTogether() {
        send("orders-flaky", "key-flaky-1", "{\"mode\":\"throw\"}");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isGreaterThanOrEqualTo(1));
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isZero());

        send("orders-flaky", "key-flaky-2", "{\"mode\":\"ok\"}");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1));
    }

    /**
     * Two genuinely concurrent deliveries of the same idempotency key,
     * consumed by two different listener container threads (one per
     * partition, {@code concurrency = 2}) - proving Postgres's native
     * concurrency (a second {@code reserve()} blocks on the reservation
     * row's own lock, ADR 0001/0002/Slice 018) holds through the full
     * Kafka stack, not just the store in isolation ({@code
     * PostgresNativeConcurrencyTest}) or the HTTP stack ({@code
     * PostgresIdempotencyEndToEndTest}).
     */
    @Test
    void twoConcurrentDeliveriesOfTheSameKeyAcrossPartitionsExecuteTheListenerExactlyOnce() {
        ProducerRecord<String, String> first = new ProducerRecord<>("orders-concurrent", 0, "record-key", "{\"amount\":10}");
        first.headers().add(HEADER, "concurrent-key".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<String, String> second = new ProducerRecord<>("orders-concurrent", 1, "record-key", "{\"amount\":10}");
        second.headers().add(HEADER, "concurrent-key".getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(first);
        kafkaTemplate.send(second);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
    }

    /**
     * When the primary of two concurrent deliveries throws, the blocked
     * partner self-promotes to owner (ADR 0001/0002's "resolves according to
     * what the primary does with its transaction") and executes the effect
     * itself - proving "exactly once", not "zero or twice", the same
     * scenario {@code PostgresIdempotencyEndToEndTest
     * .whenThePrimaryThrowsTheBlockedConcurrentRequestSelfPromotesAndExecutesTheEffectExactlyOnce}
     * proves for HTTP. This listener's dedicated {@code
     * concurrentTestContainerFactory} never redelivers (see its javadoc) -
     * the primary's throw is final, so the only two invocations are the
     * primary's (failed) attempt and the waiter's self-promoted one; nothing
     * races back in afterwards to complicate the outcome.
     */
    @Test
    void whenThePrimaryThrowsTheBlockedConcurrentDeliverySelfPromotesAndExecutesTheListenerExactlyOnce() {
        ProducerRecord<String, String> primary = new ProducerRecord<>("orders-concurrent", 0, "record-key", "{\"mode\":\"throw\"}");
        primary.headers().add(HEADER, "concurrent-key-throw".getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(primary);

        // Two independently-sent messages to different partitions have no
        // ordering guarantee - which one's consumer thread actually reaches
        // reserve() first is otherwise a coin flip. Waiting for the primary
        // to have won the reservation and entered its sleep (listenerInvocations
        // increments before the sleep, in onConcurrent) before sending the
        // waiter forces it to be the one left blocking on the still-open
        // transaction, rather than leaving "who becomes primary" to chance.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(listenerInvocations.get()).isEqualTo(1));

        ProducerRecord<String, String> waiter = new ProducerRecord<>("orders-concurrent", 1, "record-key", "{\"mode\":\"ok\"}");
        waiter.headers().add(HEADER, "concurrent-key-throw".getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(waiter);

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
        send("orders-no-transactional", "key-no-tx-1", "{\"mode\":\"error\"}");

        // See aThrownExceptionRollsBackTheReservationAndTheListenersOwnWriteTogether's
        // javadoc for why this doesn't pin an exact invocation count.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(listenerInvocations.get()).isGreaterThanOrEqualTo(1));
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isZero());

        send("orders-no-transactional", "key-no-tx-2", "{\"mode\":\"ok\"}");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1));
    }

    private void send(String topic, String idempotencyKey, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, "record-key", payload);
        record.headers().add(HEADER, idempotencyKey.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableKafka
    static class TestApplication {

        @Bean
        AtomicInteger listenerInvocations() {
            return new AtomicInteger();
        }

        @Bean
        OrdersListener ordersListener(JdbcTemplate jdbc, AtomicInteger listenerInvocations) {
            return new OrdersListener(jdbc, listenerInvocations);
        }

        @Bean
        ProducerFactory<String, String> producerFactory(@Value("${spring.embedded.kafka.brokers}") String brokers) {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(config);
        }

        @Bean
        KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }

        @Bean
        ConsumerFactory<String, String> consumerFactory(@Value("${spring.embedded.kafka.brokers}") String brokers) {
            Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "idempotency-postgres-test");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return new DefaultKafkaConsumerFactory<>(config);
        }

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
                ConsumerFactory<String, String> consumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            // Bounds the "flaky"/"no-transactional" listeners' thrown-exception
            // redelivery so those tests' retries don't run forever.
            factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(100L, 2)));
            return factory;
        }

        /**
         * Dedicated factory for the "orders-concurrent" listener only: two
         * partitions/two threads (independent of the default factory's
         * concurrency) so the same-key deliveries in the concurrency tests
         * race genuinely concurrently, with no redelivery at all ({@code
         * FixedBackOff(0, 0)}) - a thrown exception there is final, exactly
         * like an HTTP handler's thrown exception is final with no automatic
         * retry, so the self-promotion test's outcome isn't complicated by a
         * stray redelivery racing back against the already-self-promoted
         * waiter's commit. Redelivery-driven rollback is proven separately,
         * on the default factory above.
         */
        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String> concurrentTestContainerFactory(
                ConsumerFactory<String, String> consumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.setConcurrency(2);
            factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
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
        @KafkaListener(id = "orders-listener", topics = "orders")
        @Transactional
        void onOrder(ConsumerRecord<String, String> record) {
            listenerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 10);
        }

        @Idempotent(header = HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @KafkaListener(id = "orders-flaky-listener", topics = "orders-flaky")
        @Transactional
        void onFlaky(ConsumerRecord<String, String> record) {
            listenerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 999);
            if (record.value().contains("throw")) {
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
        @KafkaListener(id = "orders-concurrent-listener", topics = "orders-concurrent", containerFactory = "concurrentTestContainerFactory")
        @Transactional
        void onConcurrent(ConsumerRecord<String, String> record) throws InterruptedException {
            listenerInvocations.incrementAndGet();
            Thread.sleep(300);
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 1);
            if (record.value().contains("throw")) {
                throw new IllegalStateException("boom");
            }
        }

        /** No {@code @Transactional} at all - see {@code aThrownExceptionInAListenerWithNoTransactionalAnnotationStillRollsBackTogetherWithTheReservation}. */
        @Idempotent(header = HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @KafkaListener(id = "orders-no-transactional-listener", topics = "orders-no-transactional")
        void onNoTransactional(ConsumerRecord<String, String> record) {
            listenerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 555);
            if (record.value().contains("error")) {
                throw new IllegalStateException("boom");
            }
        }
    }
}
