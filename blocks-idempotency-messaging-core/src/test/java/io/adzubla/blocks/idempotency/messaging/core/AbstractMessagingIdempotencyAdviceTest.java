package io.adzubla.blocks.idempotency.messaging.core;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.metrics.NoOpIdempotencyMetrics;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Direct unit coverage of the broker-neutral decision skeleton in {@link
 * AbstractMessagingIdempotencyAdvice}, driven through a minimal concrete
 * subclass and a {@link FakeMessageDelivery} - no broker, no Spring AOP
 * proxying. The reserve/complete/release flow runs for real against an {@link
 * InMemoryIdempotencyStore}; the two broker-specific terminal actions
 * (dead-letter, fail-closed) are observed through the fake's recorded calls.
 * The per-broker advice tests then only need to prove their own seam wiring,
 * not re-cover the whole table.
 */
class AbstractMessagingIdempotencyAdviceTest {

    private static final String HEADER = "Idempotency-Key";
    private static final String DESTINATION = "orders";
    private static final String LISTENER_ID = "test-listener";

    private InMemoryIdempotencyStore store;
    private IdempotencyEngineRegistry engineRegistry;
    private IdempotencyProperties properties;
    private TestAdvice advice;
    private AtomicInteger invocations;

    @BeforeEach
    void setUp() {
        properties = new IdempotencyProperties();
        properties.setDefaultStore(InMemoryIdempotencyStore.QUALIFIER);
        store = new InMemoryIdempotencyStore();
        engineRegistry = new IdempotencyEngineRegistry(Map.of(InMemoryIdempotencyStore.QUALIFIER, store),
                properties.getPollInterval(), properties.getPollJitter(), NoOpIdempotencyMetrics.INSTANCE);
        advice = new TestAdvice(engineRegistry, properties);
        invocations = new AtomicInteger();
    }

    @Test
    void firstDeliveryInvokesTheListenerAndCompletesTheRecord() throws Throwable {
        advice.handle(joinPointFor(delivery("key-1", "{}"), "onMessage"));

        assertThat(invocations.get()).isEqualTo(1);
    }

    @Test
    void repeatDeliveryWithTheSameKeyIsAckedWithoutReinvokingTheListener() throws Throwable {
        advice.handle(joinPointFor(delivery("key-2", "{}"), "onMessage"));
        Object second = advice.handle(joinPointFor(delivery("key-2", "{}"), "onMessage"));

        assertThat(invocations.get()).isEqualTo(1);
        assertThat(second).isNull();
    }

    @Test
    void differentKeysAreIsolated() throws Throwable {
        advice.handle(joinPointFor(delivery("key-3", "{}"), "onMessage"));
        advice.handle(joinPointFor(delivery("key-4", "{}"), "onMessage"));

        assertThat(invocations.get()).isEqualTo(2);
    }

    @Test
    void missingRequiredKeyIsDeadLetteredInsteadOfInvokingTheListener() throws Throwable {
        FakeMessageDelivery delivery = delivery(null, "{\"amount\":10}");

        Object result = advice.handle(joinPointFor(delivery, "onMessage"));

        assertThat(invocations.get()).isZero();
        assertThat(delivery.deadLetterCalls).isEqualTo(1);
        assertThat(delivery.lastDeadLetterReason).isEqualTo("key missing but required");
        assertThat(delivery.lastDeadLetterValue).isEqualTo("n/a");
        assertThat(result).isSameAs(FakeMessageDelivery.DEAD_LETTERED);
    }

    @Test
    void missingKeyWithKeyOptionalPassesThroughUnprotected() throws Throwable {
        FakeMessageDelivery delivery = delivery(null, "{}");

        advice.handle(joinPointFor(delivery, "onMessageOptionalKey"));

        assertThat(invocations.get()).isEqualTo(1);
        assertThat(delivery.deadLetterCalls).isZero();
    }

    @Test
    void invalidKeyIsDeadLetteredInsteadOfInvokingTheListener() throws Throwable {
        FakeMessageDelivery delivery = delivery("not a valid key!", "{\"amount\":10}");

        advice.handle(joinPointFor(delivery, "onMessage"));

        assertThat(invocations.get()).isZero();
        assertThat(delivery.deadLetterCalls).isEqualTo(1);
        assertThat(delivery.lastDeadLetterReason).isEqualTo("key value invalid (size/charset)");
        assertThat(delivery.lastDeadLetterValue).isEqualTo("not a valid key!");
    }

    @Test
    void collisionWithADifferentBodyIsDeadLetteredInsteadOfInvokingTheListener() throws Throwable {
        advice.handle(joinPointFor(delivery("key-collision", "{\"amount\":10}"), "onMessage"));
        assertThat(invocations.get()).isEqualTo(1);

        FakeMessageDelivery collision = delivery("key-collision", "{\"amount\":20}");
        advice.handle(joinPointFor(collision, "onMessage"));

        assertThat(invocations.get()).isEqualTo(1);
        assertThat(collision.deadLetterCalls).isEqualTo(1);
        assertThat(collision.lastDeadLetterReason).isEqualTo("collision (fingerprint mismatch)");
        assertThat(collision.lastDeadLetterValue).isEqualTo("key-collision");
    }

    @Test
    void concurrentDuplicateOfAnInProgressKeyIsAckedWithoutInvokingTheListener() throws Throwable {
        String body = "{\"amount\":10}";
        // Reserve the key directly, leaving it IN_PROGRESS (never completed) - stands in for a
        // primary delivery still mid-processing when a concurrent duplicate of the same key arrives.
        EffectiveKey key = MessagingEffectiveKeyFactory.create(DESTINATION, LISTENER_ID, "key-race");
        String fingerprint = Fingerprint.sha256(key.route(), key.handler(), body.getBytes(StandardCharsets.UTF_8));
        engineRegistry.engine(InMemoryIdempotencyStore.QUALIFIER).before(key, fingerprint, properties.getLockTtl(),
                OnStoreFailure.OPEN, WhenInProgress.REJECT, properties.getWaitTimeout());

        Object result = advice.handle(joinPointFor(delivery("key-race", body), "onMessage"));

        assertThat(result).isNull();
        assertThat(invocations.get()).isZero();
        assertThat(store.find(key)).hasValueSatisfying(record -> assertThat(record.state()).isEqualTo(RecordState.IN_PROGRESS));
    }

    @Test
    void storeUnavailableWithFailOpenLetsTheListenerRunUnprotected() throws Throwable {
        store.setUnavailable(true);

        advice.handle(joinPointFor(delivery("key-outage", "{}"), "onMessage"));

        assertThat(invocations.get()).isEqualTo(1);
    }

    @Test
    void storeUnavailableWithFailClosedInvokesTheFailClosedSeamRatherThanTheListener() throws Throwable {
        store.setUnavailable(true);
        FakeMessageDelivery delivery = delivery("key-outage-closed", "{}");

        assertThatThrownBy(() -> advice.handle(joinPointFor(delivery, "onMessageFailClosed")))
                .isInstanceOf(IllegalStateException.class);

        assertThat(invocations.get()).isZero();
        assertThat(delivery.failClosedCalls).isEqualTo(1);
    }

    private FakeMessageDelivery delivery(String rawKey, String body) {
        return new FakeMessageDelivery(DESTINATION, LISTENER_ID, body.getBytes(StandardCharsets.UTF_8), rawKey);
    }

    private ProceedingJoinPoint joinPointFor(FakeMessageDelivery delivery, String methodName) throws Throwable {
        Method method = Listener.class.getDeclaredMethod(methodName, String.class);
        MethodSignature signature = mock(MethodSignature.class);
        when(signature.getMethod()).thenReturn(method);

        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(joinPoint.proceed()).thenAnswer(invocation -> {
            invocations.incrementAndGet();
            return null;
        });
        advice.nextDelivery = delivery;
        return joinPoint;
    }

    /** Concrete subclass exposing the shared skeleton, returning the {@link FakeMessageDelivery} the test staged. */
    private static final class TestAdvice extends AbstractMessagingIdempotencyAdvice {
        private MessageDelivery nextDelivery;

        private TestAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties) {
            super(engineRegistry, properties);
        }

        @Override
        protected MessageDelivery deliveryOf(ProceedingJoinPoint joinPoint, Method method) {
            return nextDelivery;
        }
    }

    /** Records the broker-specific terminal actions the skeleton delegates, so the test can assert which fired. */
    private static final class FakeMessageDelivery implements MessageDelivery {
        private static final Object DEAD_LETTERED = new Object();

        private final String destination;
        private final String listenerId;
        private final byte[] body;
        private final String rawKey;

        private int deadLetterCalls;
        private String lastDeadLetterReason;
        private String lastDeadLetterValue;
        private int failClosedCalls;

        private FakeMessageDelivery(String destination, String listenerId, byte[] body, String rawKey) {
            this.destination = destination;
            this.listenerId = listenerId;
            this.body = body;
            this.rawKey = rawKey;
        }

        @Override
        public String destination() {
            return destination;
        }

        @Override
        public String listenerId() {
            return listenerId;
        }

        @Override
        public byte[] body() {
            return body;
        }

        @Override
        public Optional<String> resolveHeaderKey(String headerName) {
            return Optional.ofNullable(rawKey);
        }

        @Override
        public Object deadLetter(String reason, String value) {
            deadLetterCalls++;
            lastDeadLetterReason = reason;
            lastDeadLetterValue = value;
            return DEAD_LETTERED;
        }

        @Override
        public Object failClosed() {
            failClosedCalls++;
            throw new IllegalStateException("fail-closed: store unavailable for " + destination + "/" + listenerId);
        }
    }

    @SuppressWarnings("unused")
    static class Listener {
        @Idempotent(header = HEADER)
        void onMessage(String payload) {
        }

        @Idempotent(header = HEADER, keyRequired = false)
        void onMessageOptionalKey(String payload) {
        }

        @Idempotent(header = HEADER, onStoreFailure = OnStoreFailure.CLOSED)
        void onMessageFailClosed(String payload) {
        }
    }
}
