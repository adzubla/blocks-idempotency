package io.adzubla.blocks.idempotency.config;

import io.adzubla.blocks.idempotency.key.DefaultPrincipalClaimResolver;
import io.adzubla.blocks.idempotency.key.PrincipalClaimResolver;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.security.Principal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the {@code idempotency.scope.principal-claim} wiring's bean-selection
 * contract: {@link DefaultPrincipalClaimResolver} by default, overridable by any
 * user-registered {@link PrincipalClaimResolver} bean.
 */
class PrincipalClaimResolverAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(StoreConfig.class);

    @Test
    void noUserBeanFallsBackToDefaultResolverWithoutFailingStartup() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(PrincipalClaimResolver.class)).isInstanceOf(DefaultPrincipalClaimResolver.class);
        });
    }

    @Test
    void userSuppliedResolverWinsOverTheDefault() {
        runner.withUserConfiguration(CustomResolverConfig.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(PrincipalClaimResolver.class)).isInstanceOf(CustomPrincipalClaimResolver.class);
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class StoreConfig {
        @Bean
        IdempotencyStore idempotencyStore() {
            return new InMemoryIdempotencyStore();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomResolverConfig {
        @Bean
        PrincipalClaimResolver customPrincipalClaimResolver() {
            return new CustomPrincipalClaimResolver();
        }
    }

    /** A distinct implementation - not {@link DefaultPrincipalClaimResolver} - to prove identity, not just fallback behavior. */
    static class CustomPrincipalClaimResolver implements PrincipalClaimResolver {
        @Override
        public String resolve(Principal principal, String claim) {
            return "custom-" + claim;
        }
    }
}
