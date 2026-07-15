package io.adzubla.blocks.idempotency.key;

import java.security.Principal;

/**
 * Resolves the string used as the effective key's principal, given the
 * configured {@code idempotency.scope.principal-claim}. Implement as a Spring
 * bean to scope by something other than {@link Principal#getName()} (e.g. a
 * JWT claim) - the {@code claim} argument is passed through verbatim from
 * configuration and is otherwise meaningless to the library itself.
 */
@FunctionalInterface
public interface PrincipalClaimResolver {

    /** Return {@code null} to fall back to {@link Principal#getName()}. */
    String resolve(Principal principal, String claim);
}
