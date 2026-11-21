package io.adzubla.blocks.idempotency.messaging.core;

import io.adzubla.blocks.idempotency.key.EffectiveKeyFactory;
import io.adzubla.blocks.idempotency.model.EffectiveKey;

/**
 * Composes the {@link EffectiveKey} for a message-listener delivery: route =
 * the destination (a Kafka topic, a RabbitMQ queue, or a JMS queue/topic), handler = the listener
 * id, then delegates to the transport-neutral {@link EffectiveKeyFactory} to
 * build it (see {@code CONTEXT.md} — Key scope). {@code principal} is always
 * {@link EffectiveKey#NO_PRINCIPAL} — no servlet-principal equivalent exists
 * for a message listener (per the messaging-extension PRD, v1 scope).
 *
 * <p>Shared by every broker: the only per-broker difference is the name of the
 * {@code destination} argument (topic vs. queue).
 */
public final class MessagingEffectiveKeyFactory {

    private MessagingEffectiveKeyFactory() {
    }

    public static EffectiveKey create(String destination, String listenerId, String rawKey) {
        return EffectiveKeyFactory.create(destination, listenerId, EffectiveKey.NO_PRINCIPAL, rawKey);
    }
}
