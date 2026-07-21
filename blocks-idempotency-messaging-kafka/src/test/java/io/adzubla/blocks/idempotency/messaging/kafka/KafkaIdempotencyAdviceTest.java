package io.adzubla.blocks.idempotency.messaging.kafka;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
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
import java.util.concurrent.atomic.AtomicInteger;

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
    void missingRequiredKeyThrowsRatherThanInvokingTheListener() throws Throwable {
        RecordHeaders headers = new RecordHeaders();
        ConsumerRecord<String, String> record = consumerRecord(headers);

        assertThatThrownBy(() -> advice.aroundIdempotentListener(joinPointFor(record)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(invocations.get()).isZero();
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
    void storeUnavailableWithFailOpenLetsTheListenerRunUnprotected() throws Throwable {
        store.setUnavailable(true);

        advice.aroundIdempotentListener(joinPointFor("key-outage"));

        assertThat(invocations.get()).isEqualTo(1);
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
}
