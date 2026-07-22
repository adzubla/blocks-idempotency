package io.adzubla.blocks.idempotency.messaging.rabbitmq.key;

import io.adzubla.blocks.idempotency.key.EffectiveKeyFactory;
import io.adzubla.blocks.idempotency.model.EffectiveKey;

/**
 * Composes the {@link EffectiveKey} for a RabbitMQ delivery: route = the
 * destination queue, handler = the listener id, then delegates to the
 * transport-neutral {@link EffectiveKeyFactory} to build it (see {@code
 * CONTEXT.md} — Key scope). {@code principal} is always {@link
 * EffectiveKey#NO_PRINCIPAL} — no servlet-principal equivalent exists for a
 * message listener (per the messaging-extension PRD, v1 scope).
 */
public final class RabbitEffectiveKeyFactory {

    private RabbitEffectiveKeyFactory() {
    }

    public static EffectiveKey create(String queue, String listenerId, String rawKey) {
        return EffectiveKeyFactory.create(queue, listenerId, EffectiveKey.NO_PRINCIPAL, rawKey);
    }
}
