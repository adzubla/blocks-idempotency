package io.adzubla.blocks.idempotency.key;

import java.security.Principal;

/** Default {@link PrincipalClaimResolver}: ignores {@code claim}, always uses {@link Principal#getName()}. */
public final class DefaultPrincipalClaimResolver implements PrincipalClaimResolver {

    @Override
    public String resolve(Principal principal, String claim) {
        return principal.getName();
    }
}
