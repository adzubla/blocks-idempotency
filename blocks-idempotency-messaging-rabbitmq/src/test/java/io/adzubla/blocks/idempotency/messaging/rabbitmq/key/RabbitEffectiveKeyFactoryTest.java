package io.adzubla.blocks.idempotency.messaging.rabbitmq.key;

import io.adzubla.blocks.idempotency.model.EffectiveKey;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitEffectiveKeyFactoryTest {

    @Test
    void scopesTheKeyByQueueListenerAndValueWithNoPrincipal() {
        EffectiveKey key = RabbitEffectiveKeyFactory.create("orders-queue", "orders-listener", "key-1");

        assertThat(key.route()).isEqualTo("orders-queue");
        assertThat(key.handler()).isEqualTo("orders-listener");
        assertThat(key.principal()).isEqualTo(EffectiveKey.NO_PRINCIPAL);
        assertThat(key.value()).isEqualTo("key-1");
    }

    @Test
    void differentQueuesAreIsolatedForTheSameListenerAndKey() {
        EffectiveKey a = RabbitEffectiveKeyFactory.create("orders-queue", "shared-listener", "key-1");
        EffectiveKey b = RabbitEffectiveKeyFactory.create("payments-queue", "shared-listener", "key-1");

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void differentListenersAreIsolatedForTheSameQueueAndKey() {
        EffectiveKey a = RabbitEffectiveKeyFactory.create("orders-queue", "listener-a", "key-1");
        EffectiveKey b = RabbitEffectiveKeyFactory.create("orders-queue", "listener-b", "key-1");

        assertThat(a).isNotEqualTo(b);
    }
}
