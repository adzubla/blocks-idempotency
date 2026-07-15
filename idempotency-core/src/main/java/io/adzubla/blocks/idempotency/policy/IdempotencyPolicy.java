package io.adzubla.blocks.idempotency.policy;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.annotation.Idempotent.WhenInProgress;

import java.time.Duration;

/**
 * The effective per-request policy resolved by {@link PolicyResolver}: every
 * inheritable {@link Idempotent} attribute already settled to either its
 * explicit value or the global default - never a sentinel.
 */
public record IdempotencyPolicy(
        String store,
        Duration ttl,
        OnStoreFailure onStoreFailure,
        WhenInProgress whenInProgress,
        boolean keyRequired) {
}
