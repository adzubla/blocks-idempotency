package io.adzubla.blocks.idempotency.messaging.jms;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.messaging.core.MessagingEffectiveKeyFactory;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit coverage of {@link JmsIdempotencyAdvice} driven directly (no Spring AOP
 * proxying, no broker) - a mocked {@link ProceedingJoinPoint} stands in for
 * the intercepted listener invocation, wired to a real {@link
 * IdempotencyEngineRegistry} over {@link InMemoryIdempotencyStore} so the
 * reserve/complete flow is exercised for real.
 */
class JmsIdempotencyAdviceTest {

    private static final String HEADER = "IdempotencyKey";
    private static final String DESTINATION = "orders";
    private static final String LISTENER_ID = "test-listener";

    private InMemoryIdempotencyStore store;
    private JmsTemplate deadLetterTemplate;
    private JmsIdempotencyAdvice advice;
    private AtomicInteger invocations;

    @BeforeEach
    void setUp() {
        IdempotencyProperties properties = new IdempotencyProperties();
        properties.setDefaultStore(InMemoryIdempotencyStore.QUALIFIER);
        store = new InMemoryIdempotencyStore();
        IdempotencyEngineRegistry engineRegistry = new IdempotencyEngineRegistry(
                Map.of(InMemoryIdempotencyStore.QUALIFIER, store), properties.getPollInterval(), properties.getPollJitter(),
                NoOpIdempotencyMetrics.INSTANCE);
        deadLetterTemplate = mock(JmsTemplate.class);
        JmsDeadLetterPublisher deadLetterPublisher = new JmsDeadLetterPublisher(deadLetterTemplate, ".DLQ");
        advice = new JmsIdempotencyAdvice(engineRegistry, properties, deadLetterPublisher);
        invocations = new AtomicInteger();
    }

    @Test
    void firstDeliveryInvokesTheListenerAndCompletesTheRecord() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor("key-1", "{}"));

        assertThat(invocations.get()).isEqualTo(1);
    }

    @Test
    void repeatDeliveryWithTheSameKeyIsAckedWithoutReinvokingTheListener() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor("key-2", "{}"));
        Object second = advice.aroundIdempotentListener(joinPointFor("key-2", "{}"));

        assertThat(invocations.get()).isEqualTo(1);
        assertThat(second).isNull();
    }

    @Test
    void differentKeysAreIsolated() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor("key-3", "{}"));
        advice.aroundIdempotentListener(joinPointFor("key-4", "{}"));

        assertThat(invocations.get()).isEqualTo(2);
    }

    @Test
    void effectiveKeyIsScopedByDestinationListenerAndValueWithNoPrincipal() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor("key-5", "{\"amount\":10}"));

        EffectiveKey key = MessagingEffectiveKeyFactory.create(DESTINATION, LISTENER_ID, "key-5");
        assertThat(key.principal()).isEqualTo(EffectiveKey.NO_PRINCIPAL);
        assertThat(store.find(key)).hasValueSatisfying(record -> assertThat(record.state()).isEqualTo(RecordState.COMPLETED));
    }

    @Test
    void missingKeyWithKeyOptionalPassesThroughUnprotected() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor(null, "{}", OptionalKeyListener.class));

        assertThat(invocations.get()).isEqualTo(1);
    }

    @Test
    void storeUnavailableWithFailOpenLetsTheListenerRunUnprotected() throws Throwable {
        store.setUnavailable(true);

        advice.aroundIdempotentListener(joinPointFor("key-outage", "{}"));

        assertThat(invocations.get()).isEqualTo(1);
    }

    @Test
    void missingRequiredKeyIsRoutedToTheDeadLetterDestinationInsteadOfInvokingTheListener() throws Throwable {
        Object result = advice.aroundIdempotentListener(joinPointFor(null, "{\"amount\":10}"));

        assertThat(result).isNull();
        assertThat(invocations.get()).isZero();
        verify(deadLetterTemplate, times(1)).send(eq("orders.DLQ"), any(MessageCreator.class));
    }

    @Test
    void invalidKeyIsRoutedToTheDeadLetterDestinationInsteadOfInvokingTheListener() throws Throwable {
        Object result = advice.aroundIdempotentListener(joinPointFor("not a valid key!", "{\"amount\":10}"));

        assertThat(result).isNull();
        assertThat(invocations.get()).isZero();
        verify(deadLetterTemplate, times(1)).send(eq("orders.DLQ"), any(MessageCreator.class));
    }

    @Test
    void collisionWithADifferentBodyIsRoutedToTheDeadLetterDestinationInsteadOfInvokingTheListener() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor("key-collision", "{\"amount\":10}"));
        assertThat(invocations.get()).isEqualTo(1);

        Object result = advice.aroundIdempotentListener(joinPointFor("key-collision", "{\"amount\":20}"));

        assertThat(result).isNull();
        // The original delivery's own completion is unaffected - the listener never ran a second time.
        assertThat(invocations.get()).isEqualTo(1);
        verify(deadLetterTemplate, times(1)).send(eq("orders.DLQ"), any(MessageCreator.class));
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
        EffectiveKey key = MessagingEffectiveKeyFactory.create(DESTINATION, LISTENER_ID, idempotencyKey);
        String fingerprint = Fingerprint.sha256(key.route(), key.handler(), body.getBytes(StandardCharsets.UTF_8));
        assertThat(store.find(key)).hasValueSatisfying(record -> {
            assertThat(record.state()).isEqualTo(RecordState.COMPLETED);
            assertThat(record.fingerprint()).isEqualTo(fingerprint);
        });
    }

    @Test
    void storeUnavailableWithFailClosedThrowsRatherThanInvokingTheListener() throws Throwable {
        store.setUnavailable(true);

        assertThatThrownBy(
                () -> advice.aroundIdempotentListener(joinPointFor("key-outage-closed", "{}", FailClosedListener.class)))
                .isInstanceOf(IllegalStateException.class);

        assertThat(invocations.get()).isZero();
    }

    @Test
    void aBytesMessageBodyIsResetAfterFingerprintingSoTheListenerCanStillReadIt() throws Throwable {
        byte[] payload = "{\"amount\":10}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Queue queue = mock(Queue.class);
        when(queue.getQueueName()).thenReturn(DESTINATION);
        jakarta.jms.BytesMessage message = mock(jakarta.jms.BytesMessage.class);
        when(message.getJMSDestination()).thenReturn(queue);
        when(message.getStringProperty(HEADER)).thenReturn("key-bytes");
        when(message.getBodyLength()).thenReturn((long) payload.length);
        when(message.readBytes(org.mockito.ArgumentMatchers.any(byte[].class))).thenAnswer(invocation -> {
            System.arraycopy(payload, 0, invocation.getArgument(0), 0, payload.length);
            return payload.length;
        });

        Method method = Listener.class.getDeclaredMethod("onMessage", Message.class);
        MethodSignature signature = mock(MethodSignature.class);
        when(signature.getMethod()).thenReturn(method);
        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {message});
        when(joinPoint.proceed()).thenAnswer(invocation -> {
            invocations.incrementAndGet();
            return null;
        });

        advice.aroundIdempotentListener(joinPoint);

        assertThat(invocations.get()).isEqualTo(1);
        // The read cursor advanced by readBytes() is rewound, leaving the body re-readable downstream.
        org.mockito.Mockito.verify(message).reset();
    }

    private ProceedingJoinPoint joinPointFor(String key, String body) throws Throwable {
        return joinPointFor(key, body, Listener.class);
    }

    private ProceedingJoinPoint joinPointFor(String key, String body, Class<?> listenerClass) throws Throwable {
        Method method = listenerClass.getDeclaredMethod("onMessage", Message.class);
        TextMessage message = textMessage(key, body);
        MethodSignature signature = mock(MethodSignature.class);
        when(signature.getMethod()).thenReturn(method);

        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {message});
        when(joinPoint.proceed()).thenAnswer(invocation -> {
            invocations.incrementAndGet();
            return null;
        });
        return joinPoint;
    }

    /**
     * A join point whose {@code proceed()} signals {@code started} (the
     * point at which the reservation already exists, since {@code before()}
     * runs before {@code proceed()} is ever called) and then blocks on
     * {@code release} before incrementing {@code invocations} - simulating a
     * primary listener invocation slow enough for a real concurrent
     * duplicate delivery to observe it mid-processing.
     */
    private ProceedingJoinPoint blockingJoinPointFor(String key, String body, CountDownLatch started, CountDownLatch release)
            throws Throwable {
        Method method = Listener.class.getDeclaredMethod("onMessage", Message.class);
        TextMessage message = textMessage(key, body);
        MethodSignature signature = mock(MethodSignature.class);
        when(signature.getMethod()).thenReturn(method);

        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {message});
        when(joinPoint.proceed()).thenAnswer(invocation -> {
            started.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            invocations.incrementAndGet();
            return null;
        });
        return joinPoint;
    }

    private TextMessage textMessage(String key, String body) throws Exception {
        Queue queue = mock(Queue.class);
        when(queue.getQueueName()).thenReturn(DESTINATION);
        TextMessage message = mock(TextMessage.class);
        when(message.getJMSDestination()).thenReturn(queue);
        when(message.getText()).thenReturn(body);
        when(message.getStringProperty(HEADER)).thenReturn(key);
        return message;
    }

    static class Listener {
        @Idempotent(header = HEADER)
        @JmsListener(id = LISTENER_ID, destination = DESTINATION)
        void onMessage(Message message) {
        }
    }

    static class OptionalKeyListener {
        @Idempotent(header = HEADER, keyRequired = false)
        @JmsListener(id = "test-listener-optional", destination = DESTINATION)
        void onMessage(Message message) {
        }
    }

    static class FailClosedListener {
        @Idempotent(header = HEADER, onStoreFailure = OnStoreFailure.CLOSED)
        @JmsListener(id = "test-listener-fail-closed", destination = DESTINATION)
        void onMessage(Message message) {
        }
    }
}
