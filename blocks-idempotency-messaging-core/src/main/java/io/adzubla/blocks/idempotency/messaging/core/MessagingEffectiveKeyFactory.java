package io.adzubla.blocks.idempotency.messaging.core;

import io.adzubla.blocks.idempotency.key.EffectiveKeyFactory;
import io.adzubla.blocks.idempotency.model.EffectiveKey;

/**
 * Composes the {@link EffectiveKey} for a message-listener delivery: route =
 * the destination (a Kafka topic or a RabbitMQ queue), handler = the listener
 * id, then delegates to the transport-neutral {@link EffectiveKeyFactory} to
 * build it (see {@code CONTEXT.md} — Key scope). {@code principal} is always
 * {@link EffectiveKey#NO_PRINCIPAL} — no servlet-principal equivalent exists
 * for a message listener (per the messaging-extension PRD, v1 scope).
 *
 * <p>Extracted (Slice 048) from the per-broker {@code KafkaEffectiveKeyFactory}
 * / {@code RabbitEffectiveKeyFactory}, which differed only in the name of the
 * {@code destination} argument (topic vs. queue) — genuinely shared, not
 * broker-specific.
 */
public final class MessagingEffectiveKeyFactory {

    private MessagingEffectiveKeyFactory() {
    }

    public static EffectiveKey create(String destination, String listenerId, String rawKey) {
        return EffectiveKeyFactory.create(destination, listenerId, EffectiveKey.NO_PRINCIPAL, rawKey);
    }
}
