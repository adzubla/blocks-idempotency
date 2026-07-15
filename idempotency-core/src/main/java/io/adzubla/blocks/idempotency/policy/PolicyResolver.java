package io.adzubla.blocks.idempotency.policy;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import org.springframework.boot.convert.DurationStyle;

import java.time.Duration;

/**
 * Resolves the effective {@link IdempotencyPolicy} for a request: each
 * inheritable {@link Idempotent} attribute left at its sentinel ({@code ""}
 * for strings, {@code DEFAULT} for enums) falls back to the configured global
 * default; an explicit attribute always wins (endpoint &gt; global).
 * {@code keyRequired} is a plain boolean and never inherits.
 */
public final class PolicyResolver {

    private PolicyResolver() {
    }

    public static IdempotencyPolicy resolve(Idempotent annotation, IdempotencyProperties properties) {
        String store = annotation.store().isEmpty() ? properties.getDefaultStore() : annotation.store();
        Duration ttl = annotation.ttl().isEmpty()
                ? properties.getDefaultTtl()
                : DurationStyle.detectAndParse(annotation.ttl());
        OnStoreFailure onStoreFailure = annotation.onStoreFailure() == OnStoreFailure.DEFAULT
                ? requireResolved(properties.getDefaultOnStoreFailure(), OnStoreFailure.DEFAULT, "idempotency.default-on-store-failure")
                : annotation.onStoreFailure();
        WhenInProgress whenInProgress = annotation.whenInProgress() == WhenInProgress.DEFAULT
                ? requireResolved(properties.getDefaultWhenInProgress(), WhenInProgress.DEFAULT, "idempotency.default-when-in-progress")
                : annotation.whenInProgress();

        return new IdempotencyPolicy(store, ttl, onStoreFailure, whenInProgress, annotation.keyRequired());
    }

    /**
     * Guards {@link IdempotencyPolicy}'s "never a sentinel" contract: a global
     * default is meant to be a concrete choice, not itself {@code DEFAULT} - a
     * misconfiguration (e.g. YAML relaxed-binding happily accepts {@code
     * DEFAULT} as an enum value) would otherwise leak the sentinel straight
     * into the resolved policy.
     */
    private static <T> T requireResolved(T globalDefault, T sentinel, String property) {
        if (globalDefault == sentinel) {
            throw new IllegalStateException(property + " must not be DEFAULT");
        }
        return globalDefault;
    }
}
