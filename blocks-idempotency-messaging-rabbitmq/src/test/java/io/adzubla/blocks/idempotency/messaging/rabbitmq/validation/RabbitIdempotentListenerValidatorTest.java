package io.adzubla.blocks.idempotency.messaging.rabbitmq.validation;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of {@link RabbitIdempotentListenerValidator} via real
 * Spring context startup (an {@code ApplicationContextRunner}), mirroring
 * {@code IdempotentHandlerValidatorTest}'s and the Kafka module's own
 * {@code KafkaIdempotentListenerValidatorTest}'s "context fails to start for
 * each misconfiguration" style. No {@code @EnableRabbit}/broker connectivity
 * is needed - the validator scans bean classes directly (see its javadoc),
 * so {@code @RabbitListener} methods only need to exist on a bean, not
 * actually be processed into a listener container.
 */
class RabbitIdempotentListenerValidatorTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(AutoConfig.class)
            .withBean("in-memory", IdempotencyStore.class, InMemoryIdempotencyStore::new)
            .withPropertyValues("idempotency.default-store=in-memory");

    @Test
    void explicitWhenInProgressWaitFailsStartup() {
        runner.withUserConfiguration(ExplicitWaitListener.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining("whenInProgress=WAIT");
        });
    }

    @Test
    void whenInProgressWaitInheritedFromTheGlobalDefaultFailsStartup() {
        runner.withPropertyValues("idempotency.default-when-in-progress=WAIT")
                .withUserConfiguration(DefaultWhenInProgressListener.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("whenInProgress=WAIT");
                });
    }

    @Test
    void whenInProgressGlobalDefaultLeftAsTheSentinelFailsStartup() {
        // idempotency.default-when-in-progress=DEFAULT is a misconfiguration YAML
        // relaxed-binding happily accepts as an enum value - it must be caught here,
        // at startup, the same "never a sentinel" contract PolicyResolver.requireResolved
        // enforces for the runtime path, rather than resolving silently through to a
        // real delivery.
        runner.withPropertyValues("idempotency.default-when-in-progress=DEFAULT")
                .withUserConfiguration(DefaultWhenInProgressListener.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("default-when-in-progress").hasMessageContaining("DEFAULT");
                });
    }

    @Test
    void noStoreConfiguredFailsStartup() {
        new ApplicationContextRunner()
                .withUserConfiguration(AutoConfig.class, ValidListener.class)
                .withBean("in-memory", IdempotencyStore.class, InMemoryIdempotencyStore::new)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("no store configured")
                            .hasMessageContaining("idempotency.default-store");
                });
    }

    @Test
    void bothHeaderAndFieldPathSetFailsStartup() {
        runner.withUserConfiguration(BothHeaderAndFieldPathListener.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining("exactly one").hasMessageContaining("both");
        });
    }

    @Test
    void neitherHeaderNorFieldPathSetFailsStartup() {
        runner.withUserConfiguration(NeitherHeaderNorFieldPathListener.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining("exactly one").hasMessageContaining("neither");
        });
    }

    @Test
    void invalidTtlFailsStartup() {
        runner.withUserConfiguration(InvalidTtlListener.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining("ttl").hasMessageContaining("not-a-duration");
        });
    }

    @Test
    void unknownStoreQualifierFailsStartupNamingTheMissingModule() {
        runner.withUserConfiguration(UnknownStoreListener.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasMessageContaining("postgres")
                    .hasMessageContaining("idempotency-store-postgres");
        });
    }

    @Test
    void validConfigurationStartsNormally() {
        runner.withUserConfiguration(ValidListener.class).run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void explicitWhenInProgressRejectStartsNormallyEvenWithAWaitGlobalDefault() {
        runner.withPropertyValues("idempotency.default-when-in-progress=WAIT")
                .withUserConfiguration(ExplicitRejectListener.class)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class AutoConfig {
    }

    @Component
    static class ExplicitWaitListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, whenInProgress = WhenInProgress.WAIT)
        @RabbitListener(id = "explicit-wait", queues = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class DefaultWhenInProgressListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @RabbitListener(id = "default-when-in-progress", queues = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class ExplicitRejectListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, whenInProgress = WhenInProgress.REJECT)
        @RabbitListener(id = "explicit-reject", queues = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class BothHeaderAndFieldPathListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, fieldPath = "$.id")
        @RabbitListener(id = "both", queues = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class NeitherHeaderNorFieldPathListener {
        @Idempotent
        @RabbitListener(id = "neither", queues = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class InvalidTtlListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, ttl = "not-a-duration")
        @RabbitListener(id = "bad-ttl", queues = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class UnknownStoreListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = "postgres")
        @RabbitListener(id = "bad-store", queues = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class ValidListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @RabbitListener(id = "valid", queues = "orders")
        void onMessage(Message message) {
        }
    }
}
