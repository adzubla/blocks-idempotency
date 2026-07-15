package io.adzubla.blocks.idempotency.policy;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PolicyResolverTest {

    @Test
    void allSentinelValuesResolveToTheGlobalDefaults() {
        IdempotencyProperties properties = new IdempotencyProperties();
        properties.setDefaultStore("postgres");
        properties.setDefaultTtl(Duration.ofHours(2));
        properties.setDefaultOnStoreFailure(OnStoreFailure.CLOSED);
        properties.setDefaultWhenInProgress(WhenInProgress.WAIT);

        IdempotencyPolicy policy = PolicyResolver.resolve(annotationOf("allSentinels"), properties);

        assertThat(policy.store()).isEqualTo("postgres");
        assertThat(policy.ttl()).isEqualTo(Duration.ofHours(2));
        assertThat(policy.onStoreFailure()).isEqualTo(OnStoreFailure.CLOSED);
        assertThat(policy.whenInProgress()).isEqualTo(WhenInProgress.WAIT);
        assertThat(policy.keyRequired()).isTrue();
    }

    @Test
    void explicitAttributesOverrideTheGlobalDefaults() {
        IdempotencyProperties properties = new IdempotencyProperties();
        properties.setDefaultStore("redis");
        properties.setDefaultTtl(Duration.ofHours(24));
        properties.setDefaultOnStoreFailure(OnStoreFailure.OPEN);
        properties.setDefaultWhenInProgress(WhenInProgress.REJECT);

        IdempotencyPolicy policy = PolicyResolver.resolve(annotationOf("allExplicit"), properties);

        assertThat(policy.store()).isEqualTo("postgres");
        assertThat(policy.ttl()).isEqualTo(Duration.ofHours(1));
        assertThat(policy.onStoreFailure()).isEqualTo(OnStoreFailure.CLOSED);
        assertThat(policy.whenInProgress()).isEqualTo(WhenInProgress.WAIT);
        assertThat(policy.keyRequired()).isFalse();
    }

    @Test
    void keyRequiredNeverInheritsTheGlobalConfig() {
        IdempotencyProperties properties = new IdempotencyProperties();

        IdempotencyPolicy required = PolicyResolver.resolve(annotationOf("allSentinels"), properties);
        IdempotencyPolicy optional = PolicyResolver.resolve(annotationOf("keyNotRequired"), properties);

        assertThat(required.keyRequired()).isTrue();
        assertThat(optional.keyRequired()).isFalse();
    }

    @Test
    void changingAGlobalDefaultChangesTheResolvedPolicyWithNoCodeChange() {
        IdempotencyProperties defaultsA = new IdempotencyProperties();
        defaultsA.setDefaultTtl(Duration.ofHours(24));
        defaultsA.setDefaultWhenInProgress(WhenInProgress.REJECT);

        IdempotencyProperties defaultsB = new IdempotencyProperties();
        defaultsB.setDefaultTtl(Duration.ofHours(1));
        defaultsB.setDefaultWhenInProgress(WhenInProgress.WAIT);

        Idempotent sentinelAnnotation = annotationOf("allSentinels");
        IdempotencyPolicy policyA = PolicyResolver.resolve(sentinelAnnotation, defaultsA);
        IdempotencyPolicy policyB = PolicyResolver.resolve(sentinelAnnotation, defaultsB);

        assertThat(policyA.ttl()).isEqualTo(Duration.ofHours(24));
        assertThat(policyB.ttl()).isEqualTo(Duration.ofHours(1));
        assertThat(policyA.whenInProgress()).isEqualTo(WhenInProgress.REJECT);
        assertThat(policyB.whenInProgress()).isEqualTo(WhenInProgress.WAIT);
    }

    @Test
    void ttlAcceptsSpringsLenientDurationFormatNotJustIso8601() {
        IdempotencyProperties properties = new IdempotencyProperties();

        IdempotencyPolicy policy = PolicyResolver.resolve(annotationOf("lenientTtl"), properties);

        assertThat(policy.ttl()).isEqualTo(Duration.ofHours(6));
    }

    @Test
    void onlyTtlExplicitWhileOtherAttributesStaySentinel() {
        IdempotencyProperties properties = new IdempotencyProperties();
        properties.setDefaultStore("postgres");
        properties.setDefaultOnStoreFailure(OnStoreFailure.CLOSED);
        properties.setDefaultWhenInProgress(WhenInProgress.WAIT);

        IdempotencyPolicy policy = PolicyResolver.resolve(annotationOf("onlyTtlExplicit"), properties);

        assertThat(policy.ttl()).isEqualTo(Duration.ofHours(3));
        assertThat(policy.store()).isEqualTo("postgres");
        assertThat(policy.onStoreFailure()).isEqualTo(OnStoreFailure.CLOSED);
        assertThat(policy.whenInProgress()).isEqualTo(WhenInProgress.WAIT);
    }

    @Test
    void storeAndWhenInProgressExplicitWhileTtlAndOnStoreFailureStaySentinel() {
        IdempotencyProperties properties = new IdempotencyProperties();
        properties.setDefaultTtl(Duration.ofHours(5));
        properties.setDefaultOnStoreFailure(OnStoreFailure.OPEN);

        IdempotencyPolicy policy = PolicyResolver.resolve(annotationOf("storeAndWhenInProgressExplicit"), properties);

        assertThat(policy.store()).isEqualTo("postgres");
        assertThat(policy.whenInProgress()).isEqualTo(WhenInProgress.WAIT);
        assertThat(policy.ttl()).isEqualTo(Duration.ofHours(5));
        assertThat(policy.onStoreFailure()).isEqualTo(OnStoreFailure.OPEN);
    }

    @Test
    void aMisconfiguredGlobalOnStoreFailureDefaultIsRejected() {
        IdempotencyProperties properties = new IdempotencyProperties();
        properties.setDefaultOnStoreFailure(OnStoreFailure.DEFAULT);

        assertThatThrownBy(() -> PolicyResolver.resolve(annotationOf("allSentinels"), properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("idempotency.default-on-store-failure");
    }

    @Test
    void aMisconfiguredGlobalWhenInProgressDefaultIsRejected() {
        IdempotencyProperties properties = new IdempotencyProperties();
        properties.setDefaultWhenInProgress(WhenInProgress.DEFAULT);

        assertThatThrownBy(() -> PolicyResolver.resolve(annotationOf("allSentinels"), properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("idempotency.default-when-in-progress");
    }

    private static Idempotent annotationOf(String methodName) {
        try {
            Method method = Fixtures.class.getDeclaredMethod(methodName);
            return method.getAnnotation(Idempotent.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Holds annotated no-op methods so tests can obtain real {@link Idempotent} instances via reflection. */
    private static final class Fixtures {

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        void allSentinels() {
        }

        @Idempotent(
                header = Idempotent.IDEMPOTENCY_KEY_HEADER,
                store = "postgres",
                ttl = "PT1H",
                onStoreFailure = OnStoreFailure.CLOSED,
                whenInProgress = WhenInProgress.WAIT,
                keyRequired = false)
        void allExplicit() {
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, keyRequired = false)
        void keyNotRequired() {
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, ttl = "6h")
        void lenientTtl() {
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, ttl = "PT3H")
        void onlyTtlExplicit() {
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = "postgres", whenInProgress = WhenInProgress.WAIT)
        void storeAndWhenInProgressExplicit() {
        }
    }
}
