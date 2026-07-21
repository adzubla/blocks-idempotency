package io.adzubla.blocks.idempotency.key;

import io.adzubla.blocks.idempotency.model.EffectiveKey;

/**
 * Composes the {@link EffectiveKey}: route + handler + authenticated principal +
 * raw key value (see {@code CONTEXT.md} — Key scope). Transport-neutral - the
 * single construction seam every transport adapter (HTTP, and future
 * messaging modules) uses, so a real need for validation/normalization at
 * construction time has one place to land.
 */
public final class EffectiveKeyFactory {

    private EffectiveKeyFactory() {
    }

    public static EffectiveKey create(String route, String handler, String principal, String value) {
        return new EffectiveKey(route, handler, principal, value);
    }
}
