package io.adzubla.blocks.idempotency.key;

import io.adzubla.blocks.idempotency.model.EffectiveKey;
import jakarta.servlet.http.HttpServletRequest;

import java.security.Principal;

/**
 * Composes the {@link EffectiveKey}: route + handler + authenticated principal +
 * raw key value (see {@code CONTEXT.md} — Key scope).
 */
public final class EffectiveKeyFactory {

    private EffectiveKeyFactory() {
    }

    /**
     * @param principalEnabled whether to scope by the authenticated principal
     *                         ({@code idempotency.scope.principal-enabled}); when
     *                         {@code false}, or the route is unauthenticated, the
     *                         {@link EffectiveKey#NO_PRINCIPAL} sentinel is used.
     * @param principalClaim   passed through verbatim to {@code resolver}
     *                         ({@code idempotency.scope.principal-claim}).
     * @param resolver         resolves the principal string; a {@code null}
     *                         result falls back to {@link Principal#getName()}.
     */
    public static EffectiveKey create(HttpServletRequest request, String rawKey, boolean principalEnabled,
            String principalClaim, PrincipalClaimResolver resolver) {
        String principal = principalEnabled ? principalOf(request, principalClaim, resolver) : EffectiveKey.NO_PRINCIPAL;
        return new EffectiveKey(request.getRequestURI(), request.getMethod(), principal, rawKey);
    }

    /**
     * Transport-neutral factory for callers with no {@link HttpServletRequest}
     * (e.g. a future messaging adapter resolving {@code route}/{@code handler}
     * from a message destination and listener id instead).
     */
    public static EffectiveKey create(String route, String handler, String principal, String value) {
        return new EffectiveKey(route, handler, principal, value);
    }

    private static String principalOf(HttpServletRequest request, String principalClaim, PrincipalClaimResolver resolver) {
        Principal principal = request.getUserPrincipal();
        if (principal == null) {
            return EffectiveKey.NO_PRINCIPAL;
        }
        String resolved = resolver.resolve(principal, principalClaim);
        return resolved != null ? resolved : principal.getName();
    }
}
