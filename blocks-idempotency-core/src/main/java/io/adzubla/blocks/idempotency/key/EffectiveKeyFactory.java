package io.adzubla.blocks.idempotency.key;

import io.adzubla.blocks.idempotency.model.EffectiveKey;

/**
 * Composes the {@link EffectiveKey}: route + handler + authenticated principal +
 * raw key value (see {@code CONTEXT.md} — Key scope). Transport-neutral - the
 * single construction seam every transport adapter (HTTP, and the
 * messaging modules) uses, so a real need for validation/normalization at
 * construction time has one place to land.
 */
public final class EffectiveKeyFactory {

    private EffectiveKeyFactory() {
    }

    /**
     * Builds an effective key from already-resolved components.
     *
     * @param route     the request path or message destination
     * @param handler   the handler identity
     * @param principal the principal scope, or {@code EffectiveKey.NO_PRINCIPAL}
     * @param value     the raw idempotency key value
     * @return the effective key
     */
    public static EffectiveKey create(String route, String handler, String principal, String value) {
        return new EffectiveKey(route, handler, principal, value);
    }
}
