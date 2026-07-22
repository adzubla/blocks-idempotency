package io.adzubla.blocks.idempotency.messaging.jms;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
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

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit coverage of {@link JmsIdempotencyAdvice} driven directly (no Spring AOP
 * proxying, no broker) - a mocked {@link ProceedingJoinPoint} stands in for
 * the intercepted listener invocation, wired to a real {@link
 * IdempotencyEngineRegistry} over {@link InMemoryIdempotencyStore} so the
 * reserve/complete flow is exercised for real. Slice 049 is the happy-path
 * foundation; the terminal-action seams are covered from Slice 050.
 */
class JmsIdempotencyAdviceTest {

    private static final String HEADER = "IdempotencyKey";
    private static final String DESTINATION = "orders";
    private static final String LISTENER_ID = "test-listener";

    private InMemoryIdempotencyStore store;
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
        advice = new JmsIdempotencyAdvice(engineRegistry, properties);
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
}
