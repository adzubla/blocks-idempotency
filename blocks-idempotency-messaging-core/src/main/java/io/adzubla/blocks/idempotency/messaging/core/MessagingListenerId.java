package io.adzubla.blocks.idempotency.messaging.core;

import java.lang.reflect.Method;

/**
 * Resolves a listener's id: the broker listener annotation's own {@code id()}
 * if set, else a stable fallback derived from the method (identical fallback
 * logic previously duplicated across the JMS/Kafka/RabbitMQ advice classes).
 */
public final class MessagingListenerId {

    private MessagingListenerId() {
    }

    public static String resolve(String annotationId, Method method) {
        return annotationId.isEmpty() ? method.getDeclaringClass().getName() + "#" + method.getName() : annotationId;
    }
}
