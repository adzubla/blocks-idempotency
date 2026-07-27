package io.adzubla.blocks.idempotency.messaging.jms;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.messaging.core.MessagingEffectiveKeyFactory;
import io.adzubla.blocks.idempotency.messaging.jms.JmsDeadLetterPublisher;
import io.adzubla.blocks.idempotency.messaging.jms.JmsIdempotencyAdvice;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.store.redis.RedisIdempotencyStore;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Re-verifies {@code JmsIdempotencyAdviceTest}'s concurrent-duplicate
 * mechanism (Slice 050) against a real {@link RedisIdempotencyStore} instead
 * of {@code InMemoryIdempotencyStore}: two real threads racing {@link
 * JmsIdempotencyAdvice#aroundIdempotentListener} on the same key, over a real
 * (Testcontainers) Redis - proving the atomicity the mechanism depends on
 * (Redis's own Lua-script guarantee, not {@code ConcurrentHashMap.compute})
 * holds for this call path too. No embedded broker is needed here, same
 * reasoning as the in-memory version and the Kafka/RabbitMQ equivalents: the
 * race is at the advice/engine/store layer, not the broker's.
 */
@Testcontainers
class JmsRedisConcurrentDuplicateTest {

    private static final String HEADER = "IdempotencyKey";
    private static final String DESTINATION = "orders";
    private static final String LISTENER_ID = "test-listener";

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    private static LettuceConnectionFactory connectionFactory;

    @BeforeAll
    static void connect() {
        connectionFactory = new LettuceConnectionFactory(REDIS.getRedisHost(), REDIS.getRedisPort());
        connectionFactory.afterPropertiesSet();
    }

    @AfterAll
    static void disconnect() {
        connectionFactory.destroy();
    }

    @Test
    void concurrentDuplicateDeliveryOfTheSameKeyIsAckedWithoutInvokingTheListener() throws Throwable {
        StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, new ObjectMapper(), "idempotency-jms-race-test:");

        IdempotencyProperties properties = new IdempotencyProperties();
        properties.setDefaultStore(RedisIdempotencyStore.QUALIFIER);
        IdempotencyEngineRegistry engineRegistry = new IdempotencyEngineRegistry(Map.of(RedisIdempotencyStore.QUALIFIER, store),
                properties.getPollInterval(), properties.getPollJitter(), NoOpIdempotencyMetrics.INSTANCE);
        JmsTemplate deadLetterTemplate = mock(JmsTemplate.class);
        JmsIdempotencyAdvice advice = new JmsIdempotencyAdvice(engineRegistry, properties,
                new JmsDeadLetterPublisher(deadLetterTemplate, ".DLQ"));

        String idempotencyKey = "key-race-redis";
        String body = "{\"amount\":10}";
        AtomicInteger invocations = new AtomicInteger();
        CountDownLatch primaryStarted = new CountDownLatch(1);
        CountDownLatch releasePrimary = new CountDownLatch(1);
        AtomicReference<Throwable> primaryFailure = new AtomicReference<>();

        Thread primaryThread = new Thread(() -> {
            try {
                advice.aroundIdempotentListener(blockingJoinPointFor(idempotencyKey, body, primaryStarted, releasePrimary, invocations));
            } catch (Throwable t) {
                primaryFailure.set(t);
            }
        });
        primaryThread.start();
        try {
            assertThat(primaryStarted.await(10, TimeUnit.SECONDS)).isTrue();

            Object duplicateResult = advice.aroundIdempotentListener(joinPointFor(idempotencyKey, body, invocations));
            assertThat(duplicateResult).isNull();
            assertThat(invocations.get()).isZero();
        } finally {
            releasePrimary.countDown();
        }
        primaryThread.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(primaryThread.isAlive()).isFalse();
        assertThat(primaryFailure.get()).isNull();

        assertThat(invocations.get()).isEqualTo(1);
        EffectiveKey key = MessagingEffectiveKeyFactory.create(DESTINATION, LISTENER_ID, idempotencyKey);
        String fingerprint = Fingerprint.sha256(key.route(), key.handler(), body.getBytes(StandardCharsets.UTF_8));
        assertThat(store.find(key)).hasValueSatisfying(record -> {
            assertThat(record.state()).isEqualTo(RecordState.COMPLETED);
            assertThat(record.fingerprint()).isEqualTo(fingerprint);
        });
    }

    private ProceedingJoinPoint joinPointFor(String idempotencyKey, String body, AtomicInteger invocations) throws Throwable {
        ProceedingJoinPoint joinPoint = baseJoinPointFor(idempotencyKey, body);
        when(joinPoint.proceed()).thenAnswer(invocation -> {
            invocations.incrementAndGet();
            return null;
        });
        return joinPoint;
    }

    private ProceedingJoinPoint blockingJoinPointFor(String idempotencyKey, String body, CountDownLatch started, CountDownLatch release,
            AtomicInteger invocations) throws Throwable {
        ProceedingJoinPoint joinPoint = baseJoinPointFor(idempotencyKey, body);
        when(joinPoint.proceed()).thenAnswer(invocation -> {
            started.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            invocations.incrementAndGet();
            return null;
        });
        return joinPoint;
    }

    private ProceedingJoinPoint baseJoinPointFor(String idempotencyKey, String body) throws Exception {
        Queue queue = mock(Queue.class);
        when(queue.getQueueName()).thenReturn(DESTINATION);
        TextMessage message = mock(TextMessage.class);
        when(message.getJMSDestination()).thenReturn(queue);
        when(message.getText()).thenReturn(body);
        when(message.getStringProperty(HEADER)).thenReturn(idempotencyKey);

        Method method = Listener.class.getDeclaredMethod("onMessage", Message.class);
        MethodSignature signature = mock(MethodSignature.class);
        when(signature.getMethod()).thenReturn(method);

        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {message});
        return joinPoint;
    }

    static class Listener {
        @Idempotent(header = HEADER)
        @JmsListener(id = LISTENER_ID, destination = DESTINATION)
        void onMessage(Message message) {
        }
    }
}
