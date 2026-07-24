package io.adzubla.blocks.idempotency.messaging.jms.validation;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import jakarta.jms.Message;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of {@link JmsIdempotentListenerValidator} via real
 * Spring context startup (an {@code ApplicationContextRunner}), mirroring
 * the Kafka/RabbitMQ modules' own validator tests. No {@code @EnableJms}/
 * broker connectivity is needed - the validator scans bean classes directly
 * (see its base class's javadoc), so {@code @JmsListener} methods only need
 * to exist on a bean, not actually be processed into a listener container.
 */
class JmsIdempotentListenerValidatorTest {

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
    void listenerWithoutAMessageParameterFailsStartup() {
        runner.withUserConfiguration(PojoOnlyListener.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasMessageContaining("must accept a")
                    .hasMessageContaining(Message.class.getName());
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
        @JmsListener(id = "explicit-wait", destination = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class DefaultWhenInProgressListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @JmsListener(id = "default-when-in-progress", destination = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class ExplicitRejectListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, whenInProgress = WhenInProgress.REJECT)
        @JmsListener(id = "explicit-reject", destination = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class BothHeaderAndFieldPathListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, fieldPath = "$.id")
        @JmsListener(id = "both", destination = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class NeitherHeaderNorFieldPathListener {
        @Idempotent
        @JmsListener(id = "neither", destination = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class InvalidTtlListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, ttl = "not-a-duration")
        @JmsListener(id = "bad-ttl", destination = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class UnknownStoreListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = "postgres")
        @JmsListener(id = "bad-store", destination = "orders")
        void onMessage(Message message) {
        }
    }

    @Component
    static class PojoOnlyListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @JmsListener(id = "pojo-only", destination = "orders")
        void onMessage(String payload) {
        }
    }

    @Component
    static class ValidListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @JmsListener(id = "valid", destination = "orders")
        void onMessage(Message message) {
        }
    }
}
