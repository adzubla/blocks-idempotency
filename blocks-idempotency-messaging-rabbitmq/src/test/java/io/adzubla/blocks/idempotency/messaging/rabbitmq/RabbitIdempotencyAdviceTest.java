package io.adzubla.blocks.idempotency.messaging.rabbitmq;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
    void missingRequiredKeyThrowsRatherThanInvokingTheListener() throws Throwable {
        Message message = message(new MessageProperties());

        assertThatThrownBy(() -> advice.aroundIdempotentListener(joinPointFor(message)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(invocations.get()).isZero();
    }

    @Test
    void storeUnavailableWithFailOpenLetsTheListenerRunUnprotected() throws Throwable {
        store.setUnavailable(true);

        advice.aroundIdempotentListener(joinPointFor("key-outage"));

        assertThat(invocations.get()).isEqualTo(1);
    }

    @Test
    void missingKeyWithKeyOptionalPassesThroughUnprotected() throws Throwable {
        Message message = message(new MessageProperties());

        advice.aroundIdempotentListener(joinPointFor(message, OptionalKeyListener.class));

        assertThat(invocations.get()).isEqualTo(1);
    }

    private ProceedingJoinPoint joinPointFor(String idempotencyKey) throws Throwable {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setHeader(HEADER, idempotencyKey);
        return joinPointFor(message(messageProperties));
    }

    private Message message(MessageProperties messageProperties) {
        messageProperties.setConsumerQueue("orders");
        return new Message("{}".getBytes(StandardCharsets.UTF_8), messageProperties);
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
}
