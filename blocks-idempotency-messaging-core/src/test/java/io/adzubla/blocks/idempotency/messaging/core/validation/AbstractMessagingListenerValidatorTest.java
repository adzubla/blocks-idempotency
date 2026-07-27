package io.adzubla.blocks.idempotency.messaging.core.validation;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct unit coverage of the broker-neutral startup-validation rules in
 * {@link AbstractMessagingListenerValidator}, driven through a minimal
 * concrete subclass ({@link TestListenerValidator}) keyed off a broker-free
 * {@code @TestListener} marker - the shared {@code header}/{@code
 * fieldPath}/{@code ttl}/{@code store}/WAIT checks and the bean-scanning
 * mechanism are exercised once here, so the per-broker validator tests only
 * need to prove their own seams (the listener marker, the message-parameter
 * signature check, the WAIT wording). Mirrors the broker validators' "context
 * fails to start for each misconfiguration" style via an {@link
 * ApplicationContextRunner}.
 */
class AbstractMessagingListenerValidatorTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(AutoConfig.class, ValidatorConfig.class)
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
                .withUserConfiguration(AutoConfig.class, ValidatorConfig.class, ValidListener.class)
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

    @Test
    void aNonListenerIdempotentMethodIsIgnored() {
        // Only methods carrying the broker's listener marker are validated - an @Idempotent
        // method without @TestListener is not this validator's concern and must not fail startup,
        // even when it is itself misconfigured (neither header nor fieldPath here).
        runner.withUserConfiguration(NonListenerIdempotentBean.class).run(context -> assertThat(context).hasNotFailed());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class AutoConfig {
    }

    @Configuration(proxyBeanMethods = false)
    static class ValidatorConfig {
        @Bean
        TestListenerValidator testListenerValidator(ConfigurableListableBeanFactory beanFactory, IdempotencyProperties properties) {
            return new TestListenerValidator(beanFactory, properties);
        }
    }

    /** Broker-free specialization: keys off {@code @TestListener} and skips the (broker-specific) signature check. */
    static class TestListenerValidator extends AbstractMessagingListenerValidator {
        TestListenerValidator(ConfigurableListableBeanFactory beanFactory, IdempotencyProperties properties) {
            super(beanFactory, properties);
        }

        @Override
        protected boolean isListenerMethod(Method method) {
            return method.isAnnotationPresent(TestListener.class);
        }

        @Override
        protected void validateSignature(Method method) {
            // The message-parameter check is broker-specific; the broker validator tests cover it.
        }

        @Override
        protected String waitNotSupportedMessage(Method method) {
            return describe(method) + ": whenInProgress=WAIT is not supported on a @TestListener method";
        }

        @Override
        protected String brokerName() {
            return "Test";
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface TestListener {
    }

    @Component
    static class ExplicitWaitListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, whenInProgress = WhenInProgress.WAIT)
        @TestListener
        void onMessage(String payload) {
        }
    }

    @Component
    static class DefaultWhenInProgressListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @TestListener
        void onMessage(String payload) {
        }
    }

    @Component
    static class ExplicitRejectListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, whenInProgress = WhenInProgress.REJECT)
        @TestListener
        void onMessage(String payload) {
        }
    }

    @Component
    static class BothHeaderAndFieldPathListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, fieldPath = "$.id")
        @TestListener
        void onMessage(String payload) {
        }
    }

    @Component
    static class NeitherHeaderNorFieldPathListener {
        @Idempotent
        @TestListener
        void onMessage(String payload) {
        }
    }

    @Component
    static class InvalidTtlListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, ttl = "not-a-duration")
        @TestListener
        void onMessage(String payload) {
        }
    }

    @Component
    static class UnknownStoreListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = "postgres")
        @TestListener
        void onMessage(String payload) {
        }
    }

    @Component
    static class ValidListener {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @TestListener
        void onMessage(String payload) {
        }
    }

    @Component
    static class NonListenerIdempotentBean {
        @Idempotent
        void notAListener(String payload) {
        }
    }
}
