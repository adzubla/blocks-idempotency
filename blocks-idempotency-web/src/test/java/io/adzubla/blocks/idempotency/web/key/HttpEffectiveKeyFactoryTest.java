package io.adzubla.blocks.idempotency.web.key;

import io.adzubla.blocks.idempotency.key.DefaultPrincipalClaimResolver;
import io.adzubla.blocks.idempotency.key.PrincipalClaimResolver;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

class HttpEffectiveKeyFactoryTest {

    private static final PrincipalClaimResolver DEFAULT_RESOLVER = new DefaultPrincipalClaimResolver();

    @Test
    void scopesByMethodPathPrincipalAndKey() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/orders");
        request.setUserPrincipal(() -> "user-1");

        EffectiveKey key = HttpEffectiveKeyFactory.create(request, "key-1", true, "sub", DEFAULT_RESOLVER);

        assertThat(key).isEqualTo(new EffectiveKey("/orders", "POST", "user-1", "key-1"));
    }

    @Test
    void fallsBackToSentinelPrincipalWhenUnauthenticated() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/orders");

        EffectiveKey key = HttpEffectiveKeyFactory.create(request, "key-1", true, "sub", DEFAULT_RESOLVER);

        assertThat(key.principal()).isEqualTo(EffectiveKey.NO_PRINCIPAL);
    }

    @Test
    void ignoresPrincipalWhenScopeDisabled() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/orders");
        request.setUserPrincipal(() -> "user-1");

        EffectiveKey key = HttpEffectiveKeyFactory.create(request, "key-1", false, "sub", DEFAULT_RESOLVER);

        assertThat(key.principal()).isEqualTo(EffectiveKey.NO_PRINCIPAL);
    }

    @Test
    void defaultResolverIgnoresClaimAndUsesGetName() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/orders");
        request.setUserPrincipal(() -> "user-1");

        EffectiveKey key = HttpEffectiveKeyFactory.create(request, "key-1", true, "email", DEFAULT_RESOLVER);

        assertThat(key.principal()).isEqualTo("user-1");
    }

    @Test
    void customResolverValueIsUsedVerbatimAsThePrincipal() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/orders");
        request.setUserPrincipal(() -> "user-1");
        PrincipalClaimResolver resolver = (principal, claim) -> "resolved-" + claim;

        EffectiveKey key = HttpEffectiveKeyFactory.create(request, "key-1", true, "email", resolver);

        assertThat(key.principal()).isEqualTo("resolved-email");
    }

    @Test
    void resolverReturningNullFallsBackToGetName() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/orders");
        request.setUserPrincipal(() -> "user-1");
        PrincipalClaimResolver resolver = (principal, claim) -> null;

        EffectiveKey key = HttpEffectiveKeyFactory.create(request, "key-1", true, "email", resolver);

        assertThat(key.principal()).isEqualTo("user-1");
    }
}
