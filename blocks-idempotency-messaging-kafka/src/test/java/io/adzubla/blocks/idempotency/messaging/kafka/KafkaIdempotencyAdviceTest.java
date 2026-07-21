package io.adzubla.blocks.idempotency.messaging.kafka;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.messaging.kafka.key.KafkaEffectiveKeyFactory;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit coverage of {@link KafkaIdempotencyAdvice} driven directly (no Spring
 * AOP proxying, no broker) - a mocked {@link ProceedingJoinPoint} stands in
 * for the intercepted listener invocation, wired to a real {@link
 * IdempotencyEngineRegistry} over {@link InMemoryIdempotencyStore} so the
 * reserve/complete flow is exercised for real.
 */
class KafkaIdempotencyAdviceTest {

    private static final String HEADER = "Idempotency-Key";

    private InMemoryIdempotencyStore store;
    private KafkaTemplate<Object, Object> deadLetterTemplate;
    private KafkaIdempotencyAdvice advice;
    private AtomicInteger invocations;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        IdempotencyProperties properties = new IdempotencyProperties();
        properties.setDefaultStore(InMemoryIdempotencyStore.QUALIFIER);
        store = new InMemoryIdempotencyStore();
        IdempotencyEngineRegistry engineRegistry = new IdempotencyEngineRegistry(
                Map.of(InMemoryIdempotencyStore.QUALIFIER, store), properties.getPollInterval(), properties.getPollJitter(),
                NoOpIdempotencyMetrics.INSTANCE);
        deadLetterTemplate = mock(KafkaTemplate.class);
        KafkaDeadLetterPublisher deadLetterPublisher = new KafkaDeadLetterPublisher(deadLetterTemplate, ".DLT");
        advice = new KafkaIdempotencyAdvice(engineRegistry, properties, deadLetterPublisher);
        invocations = new AtomicInteger();
    }

    @Test
    void firstDeliveryInvokesTheListenerAndCompletesTheRecord() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor("key-1"));

        assertThat(invocations.get()).isEqualTo(1);
    }

    @Test
    void repeatDeliveryWithTheSameKeyIsAckedWithoutReinvokingTheListener() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor("key-2"));
        advice.aroundIdempotentListener(joinPointFor("key-2"));

        assertThat(invocations.get()).isEqualTo(1);
    }

    @Test
    void differentKeysAreIsolated() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor("key-3"));
        advice.aroundIdempotentListener(joinPointFor("key-4"));

        assertThat(invocations.get()).isEqualTo(2);
    }

    @Test
    void missingRequiredKeyIsRoutedToTheDeadLetterTopicInsteadOfInvokingTheListener() throws Throwable {
        RecordHeaders headers = new RecordHeaders();
        ConsumerRecord<String, String> record = consumerRecord(headers, "{\"amount\":10}");

        Object result = advice.aroundIdempotentListener(joinPointFor(record));

        assertThat(result).isNull();
        assertThat(invocations.get()).isZero();
        verify(deadLetterTemplate, times(1)).send(argThat((ProducerRecord<Object, Object> dlt) -> "orders.DLT".equals(dlt.topic())
                && "record-key".equals(dlt.key())
                && "{\"amount\":10}".equals(dlt.value())));
    }

    @Test
    void invalidKeyIsRoutedToTheDeadLetterTopicInsteadOfInvokingTheListener() throws Throwable {
        Object result = advice.aroundIdempotentListener(joinPointFor("not a valid key!", "{\"amount\":10}"));

        assertThat(result).isNull();
        assertThat(invocations.get()).isZero();
        verify(deadLetterTemplate, times(1)).send(argThat((ProducerRecord<Object, Object> dlt) -> "orders.DLT".equals(dlt.topic())
                && "record-key".equals(dlt.key())
                && "{\"amount\":10}".equals(dlt.value())
                && "not a valid key!".equals(new String(dlt.headers().lastHeader(HEADER).value(), StandardCharsets.UTF_8))));
    }

    @Test
    void collisionWithADifferentBodyIsRoutedToTheDeadLetterTopicInsteadOfInvokingTheListener() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor("key-collision", "{\"amount\":10}"));
        assertThat(invocations.get()).isEqualTo(1);

        advice.aroundIdempotentListener(joinPointFor("key-collision", "{\"amount\":20}"));

        assertThat(invocations.get()).isEqualTo(1);
        verify(deadLetterTemplate, times(1)).send(argThat((ProducerRecord<Object, Object> record) -> "orders.DLT".equals(record.topic())
                && "record-key".equals(record.key())
                && "{\"amount\":20}".equals(record.value())
                && "key-collision".equals(new String(record.headers().lastHeader(HEADER).value(), StandardCharsets.UTF_8))));
    }

    @Test
    void concurrentDuplicateDeliveryOfTheSameKeyIsAckedWithoutInvokingTheListener() throws Throwable {
        String idempotencyKey = "key-race";
        String body = "{\"amount\":10}";
        CountDownLatch primaryStarted = new CountDownLatch(1);
        CountDownLatch releasePrimary = new CountDownLatch(1);
        AtomicReference<Throwable> primaryFailure = new AtomicReference<>();

        Thread primaryThread = new Thread(() -> {
            try {
                advice.aroundIdempotentListener(blockingJoinPointFor(idempotencyKey, body, primaryStarted, releasePrimary));
            } catch (Throwable t) {
                primaryFailure.set(t);
            }
        });
        primaryThread.start();
        try {
            assertThat(primaryStarted.await(5, TimeUnit.SECONDS)).isTrue();

            // The primary is still mid-processing (reserved, not yet completed) - a concurrent
            // duplicate delivery of the same key finds it IN_PROGRESS.
            Object duplicateResult = advice.aroundIdempotentListener(joinPointFor(idempotencyKey, body));
            assertThat(duplicateResult).isNull();
            assertThat(invocations.get()).isZero();
        } finally {
            // Unconditional even if an assertion above threw, so the primary thread never
            // outlives the test relying solely on its own await() timeout to unblock.
            releasePrimary.countDown();
        }
        primaryThread.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(primaryThread.isAlive()).isFalse();
        assertThat(primaryFailure.get()).isNull();

        // The primary's own execution and completion are unaffected by the concurrent duplicate.
        assertThat(invocations.get()).isEqualTo(1);
        EffectiveKey key = KafkaEffectiveKeyFactory.create("orders", "test-listener", idempotencyKey);
        String fingerprint = Fingerprint.sha256(key.route(), key.handler(), body.getBytes(StandardCharsets.UTF_8));
        assertThat(store.find(key)).hasValueSatisfying(record -> {
            assertThat(record.state()).isEqualTo(RecordState.COMPLETED);
            assertThat(record.fingerprint()).isEqualTo(fingerprint);
        });
    }

    @Test
    void storeUnavailableWithFailOpenLetsTheListenerRunUnprotected() throws Throwable {
        store.setUnavailable(true);

        advice.aroundIdempotentListener(joinPointFor("key-outage"));

        assertThat(invocations.get()).isEqualTo(1);
    }

    @Test
    void storeUnavailableWithFailClosedThrowsRatherThanInvokingTheListener() throws Throwable {
        store.setUnavailable(true);

        assertThatThrownBy(() -> advice.aroundIdempotentListener(joinPointFor("key-outage-closed", "{}", FailClosedListener.class)))
                .isInstanceOf(IllegalStateException.class);

        assertThat(invocations.get()).isZero();
    }

    @Test
    void missingKeyWithKeyOptionalPassesThroughUnprotected() throws Throwable {
        RecordHeaders headers = new RecordHeaders();
        ConsumerRecord<String, String> record = consumerRecord(headers);

        advice.aroundIdempotentListener(joinPointFor(record, OptionalKeyListener.class));

        assertThat(invocations.get()).isEqualTo(1);
    }

    private ProceedingJoinPoint joinPointFor(String idempotencyKey) throws Throwable {
        return joinPointFor(idempotencyKey, "{}");
    }

    private ProceedingJoinPoint joinPointFor(String idempotencyKey, String body) throws Throwable {
        RecordHeaders headers = new RecordHeaders();
        headers.add(HEADER, idempotencyKey.getBytes(StandardCharsets.UTF_8));
        return joinPointFor(consumerRecord(headers, body));
    }

    private ProceedingJoinPoint joinPointFor(String idempotencyKey, String body, Class<?> listenerClass) throws Throwable {
        RecordHeaders headers = new RecordHeaders();
        headers.add(HEADER, idempotencyKey.getBytes(StandardCharsets.UTF_8));
        return joinPointFor(consumerRecord(headers, body), listenerClass);
    }

    /**
     * A join point whose {@code proceed()} signals {@code started} (the
     * point at which the reservation already exists, since {@code before()}
     * runs before {@code proceed()} is ever called) and then blocks on
     * {@code release} before incrementing {@code invocations} - simulating a
     * primary listener invocation slow enough for a real concurrent
     * duplicate delivery to observe it mid-processing.
     */
    private ProceedingJoinPoint blockingJoinPointFor(String idempotencyKey, String body, CountDownLatch started, CountDownLatch release)
            throws Throwable {
        RecordHeaders headers = new RecordHeaders();
        headers.add(HEADER, idempotencyKey.getBytes(StandardCharsets.UTF_8));
        Method method = Listener.class.getDeclaredMethod("onMessage", ConsumerRecord.class);
        MethodSignature signature = mock(MethodSignature.class);
        when(signature.getMethod()).thenReturn(method);

        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {consumerRecord(headers, body)});
        when(joinPoint.proceed()).thenAnswer(invocation -> {
            started.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            invocations.incrementAndGet();
            return null;
        });
        return joinPoint;
    }

    private ConsumerRecord<String, String> consumerRecord(RecordHeaders headers) {
        return consumerRecord(headers, "{}");
    }

    private ConsumerRecord<String, String> consumerRecord(RecordHeaders headers, String body) {
        return new ConsumerRecord<>("orders", 0, 0L, ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, "record-key", body, headers, Optional.empty());
    }

    private ProceedingJoinPoint joinPointFor(ConsumerRecord<?, ?> record) throws Throwable {
        return joinPointFor(record, Listener.class);
    }

    private ProceedingJoinPoint joinPointFor(ConsumerRecord<?, ?> record, Class<?> listenerClass) throws Throwable {
        Method method = listenerClass.getDeclaredMethod("onMessage", ConsumerRecord.class);
        MethodSignature signature = mock(MethodSignature.class);
        when(signature.getMethod()).thenReturn(method);

        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {record});
        when(joinPoint.proceed()).thenAnswer(invocation -> {
            invocations.incrementAndGet();
            return null;
        });
        return joinPoint;
    }

    static class Listener {
        @Idempotent(header = HEADER)
        @KafkaListener(id = "test-listener", topics = "orders")
        void onMessage(ConsumerRecord<String, String> record) {
        }
    }

    static class OptionalKeyListener {
        @Idempotent(header = HEADER, keyRequired = false)
        @KafkaListener(id = "test-listener-optional", topics = "orders")
        void onMessage(ConsumerRecord<String, String> record) {
        }
    }

    static class FailClosedListener {
        @Idempotent(header = HEADER, onStoreFailure = OnStoreFailure.CLOSED)
        @KafkaListener(id = "test-listener-fail-closed", topics = "orders")
        void onMessage(ConsumerRecord<String, String> record) {
        }
    }
}
