package io.adzubla.blocks.idempotency.validation;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of {@link IdempotentHandlerValidator} via real Spring
 * context startup (a {@code WebApplicationContextRunner}), per Slice 010's
 * "Tests assert the context fails to start for each misconfiguration."
 */
class IdempotentHandlerValidatorTest {

    // Bean named "redis" - one of the two store-resolution paths the validator
    // supports (bean name, or a @Qualifier on the @Bean factory method - see
    // storeQualifiedByBeanMethodAnnotationIsResolved for the other).
    private final WebApplicationContextRunner runner = new WebApplicationContextRunner()
            .withUserConfiguration(AutoConfig.class)
            .withBean("redis", IdempotencyStore.class, InMemoryIdempotencyStore::new)
            .withPropertyValues("idempotency.default-store=redis");

    @Test
    void bothHeaderAndFieldPathSetFailsStartup() {
        runner.withUserConfiguration(BothHeaderAndFieldPathController.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining("exactly one").hasMessageContaining("both");
        });
    }

    @Test
    void neitherHeaderNorFieldPathSetFailsStartup() {
        runner.withUserConfiguration(NeitherHeaderNorFieldPathController.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining("exactly one").hasMessageContaining("neither");
        });
    }

    @Test
    void invalidTtlFailsStartup() {
        runner.withUserConfiguration(InvalidTtlController.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining("ttl").hasMessageContaining("not-a-duration");
        });
    }

    @Test
    void unknownStoreQualifierFailsStartupNamingTheMissingModule() {
        runner.withUserConfiguration(UnknownStoreController.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasMessageContaining("postgres")
                    .hasMessageContaining("idempotency-store-postgres");
        });
    }

    @Test
    void noStoreConfiguredFailsStartup() {
        new WebApplicationContextRunner()
                .withUserConfiguration(AutoConfig.class, ValidController.class)
                .withBean("redis", IdempotencyStore.class, InMemoryIdempotencyStore::new)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("no store configured")
                            .hasMessageContaining("idempotency.default-store");
                });
    }

    @Test
    void validConfigurationStartsNormally() {
        runner.withUserConfiguration(ValidController.class).run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void storeQualifiedByBeanMethodAnnotationIsResolved() {
        // The real store modules (RedisIdempotencyStoreAutoConfiguration,
        // PostgresIdempotencyStoreAutoConfiguration) qualify their factory
        // method with @Qualifier rather than naming the bean after the
        // qualifier - a plain bean-name lookup would miss this and wrongly
        // fail startup for a correctly configured application.
        new WebApplicationContextRunner()
                .withUserConfiguration(AutoConfig.class, QualifierAnnotatedStoreConfig.class, PostgresController.class)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class AutoConfig {
    }

    @Configuration(proxyBeanMethods = false)
    static class QualifierAnnotatedStoreConfig {
        @Bean
        @Qualifier("postgres")
        IdempotencyStore postgresIdempotencyStore() {
            return new InMemoryIdempotencyStore();
        }
    }

    @RestController
    static class PostgresController {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = "postgres")
        @GetMapping("/postgres")
        ResponseEntity<Void> handle() {
            return ResponseEntity.ok().build();
        }
    }

    @RestController
    static class BothHeaderAndFieldPathController {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, fieldPath = "$.id")
        @GetMapping("/both")
        ResponseEntity<Void> handle() {
            return ResponseEntity.ok().build();
        }
    }

    @RestController
    static class NeitherHeaderNorFieldPathController {
        @Idempotent
        @GetMapping("/neither")
        ResponseEntity<Void> handle() {
            return ResponseEntity.ok().build();
        }
    }

    @RestController
    static class InvalidTtlController {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, ttl = "not-a-duration")
        @GetMapping("/bad-ttl")
        ResponseEntity<Void> handle() {
            return ResponseEntity.ok().build();
        }
    }

    @RestController
    static class UnknownStoreController {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = "postgres")
        @GetMapping("/bad-store")
        ResponseEntity<Void> handle() {
            return ResponseEntity.ok().build();
        }
    }

    @RestController
    static class ValidController {
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @GetMapping("/valid")
        ResponseEntity<Void> handle() {
            return ResponseEntity.ok().build();
        }
    }
}
