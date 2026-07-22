package io.adzubla.blocks.idempotency.messaging.rabbitmq;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.messaging.rabbitmq.key.RabbitEffectiveKeyFactory;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit coverage of {@link RabbitIdempotencyAdvice} driven directly (no
 * Spring AOP proxying, no broker) - a mocked {@link ProceedingJoinPoint}
 * stands in for the intercepted listener invocation, wired to a real {@link
 * IdempotencyEngineRegistry} over {@link InMemoryIdempotencyStore} so the
 * reserve/complete flow is exercised for real.
 */
class RabbitIdempotencyAdviceTest {

    private static final String HEADER = "Idempotency-Key";

    private InMemoryIdempotencyStore store;
    private RabbitIdempotencyAdvice advice;
    private AtomicInteger invocations;

    @BeforeEach
    void setUp() {
        IdempotencyProperties properties = new IdempotencyProperties();
        properties.setDefaultStore(InMemoryIdempotencyStore.QUALIFIER);
        store = new InMemoryIdempotencyStore();
        IdempotencyEngineRegistry engineRegistry = new IdempotencyEngineRegistry(
                Map.of(InMemoryIdempotencyStore.QUALIFIER, store), properties.getPollInterval(), properties.getPollJitter(),
                NoOpIdempotencyMetrics.INSTANCE);
        advice = new RabbitIdempotencyAdvice(engineRegistry, properties);
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
    void missingRequiredKeyIsRejectedWithoutRequeueInsteadOfInvokingTheListener() throws Throwable {
        Message message = message(new MessageProperties(), "{\"amount\":10}");

        assertThatThrownBy(() -> advice.aroundIdempotentListener(joinPointFor(message)))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);
        assertThat(invocations.get()).isZero();
    }

    @Test
    void invalidKeyIsRejectedWithoutRequeueInsteadOfInvokingTheListener() throws Throwable {
        assertThatThrownBy(() -> advice.aroundIdempotentListener(joinPointFor("not a valid key!", "{\"amount\":10}")))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);
        assertThat(invocations.get()).isZero();
    }

    @Test
    void collisionWithADifferentBodyIsRejectedWithoutRequeueInsteadOfInvokingTheListener() throws Throwable {
        advice.aroundIdempotentListener(joinPointFor("key-collision", "{\"amount\":10}"));
        assertThat(invocations.get()).isEqualTo(1);

        assertThatThrownBy(() -> advice.aroundIdempotentListener(joinPointFor("key-collision", "{\"amount\":20}")))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);

        // The original delivery's own completion is unaffected - the listener never ran a second time.
        assertThat(invocations.get()).isEqualTo(1);
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
        EffectiveKey key = RabbitEffectiveKeyFactory.create("orders", "test-listener", idempotencyKey);
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
    void storeUnavailableWithFailClosedRejectsWithoutRequeueRatherThanInvokingTheListener() throws Throwable {
        store.setUnavailable(true);

        assertThatThrownBy(
                () -> advice.aroundIdempotentListener(joinPointFor("key-outage-closed", "{}", FailClosedListener.class)))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);

        assertThat(invocations.get()).isZero();
    }

    @Test
    void missingKeyWithKeyOptionalPassesThroughUnprotected() throws Throwable {
        Message message = message(new MessageProperties(), "{}");

        advice.aroundIdempotentListener(joinPointFor(message, OptionalKeyListener.class));

        assertThat(invocations.get()).isEqualTo(1);
    }

    private ProceedingJoinPoint joinPointFor(String idempotencyKey) throws Throwable {
        return joinPointFor(idempotencyKey, "{}");
    }

    private ProceedingJoinPoint joinPointFor(String idempotencyKey, String body) throws Throwable {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setHeader(HEADER, idempotencyKey);
        return joinPointFor(message(messageProperties, body));
    }

    private ProceedingJoinPoint joinPointFor(String idempotencyKey, String body, Class<?> listenerClass) throws Throwable {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setHeader(HEADER, idempotencyKey);
        return joinPointFor(message(messageProperties, body), listenerClass);
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
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setHeader(HEADER, idempotencyKey);
        Method method = Listener.class.getDeclaredMethod("onMessage", Message.class);
        MethodSignature signature = mock(MethodSignature.class);
        when(signature.getMethod()).thenReturn(method);

        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {message(messageProperties, body)});
        when(joinPoint.proceed()).thenAnswer(invocation -> {
            started.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            invocations.incrementAndGet();
            return null;
        });
        return joinPoint;
    }

    private Message message(MessageProperties messageProperties, String body) {
        messageProperties.setConsumerQueue("orders");
        return new Message(body.getBytes(StandardCharsets.UTF_8), messageProperties);
    }

    private ProceedingJoinPoint joinPointFor(Message message) throws Throwable {
        return joinPointFor(message, Listener.class);
    }

    private ProceedingJoinPoint joinPointFor(Message message, Class<?> listenerClass) throws Throwable {
        Method method = listenerClass.getDeclaredMethod("onMessage", Message.class);
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

    static class Listener {
        @Idempotent(header = HEADER)
        @RabbitListener(id = "test-listener", queues = "orders")
        void onMessage(Message message) {
        }
    }

    static class OptionalKeyListener {
        @Idempotent(header = HEADER, keyRequired = false)
        @RabbitListener(id = "test-listener-optional", queues = "orders")
        void onMessage(Message message) {
        }
    }

    static class FailClosedListener {
        @Idempotent(header = HEADER, onStoreFailure = OnStoreFailure.CLOSED)
        @RabbitListener(id = "test-listener-fail-closed", queues = "orders")
        void onMessage(Message message) {
        }
    }
}
