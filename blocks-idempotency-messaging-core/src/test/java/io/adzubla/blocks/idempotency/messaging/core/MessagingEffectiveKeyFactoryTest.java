package io.adzubla.blocks.idempotency.messaging.core;

import io.adzubla.blocks.idempotency.model.EffectiveKey;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MessagingEffectiveKeyFactoryTest {

    @Test
    void scopesTheKeyByDestinationListenerAndValueWithNoPrincipal() {
        EffectiveKey key = MessagingEffectiveKeyFactory.create("orders", "orders-listener", "key-1");

        assertThat(key.route()).isEqualTo("orders");
        assertThat(key.handler()).isEqualTo("orders-listener");
        assertThat(key.principal()).isEqualTo(EffectiveKey.NO_PRINCIPAL);
        assertThat(key.value()).isEqualTo("key-1");
    }

    @Test
    void differentDestinationsAreIsolatedForTheSameListenerAndKey() {
        EffectiveKey a = MessagingEffectiveKeyFactory.create("orders", "shared-listener", "key-1");
        EffectiveKey b = MessagingEffectiveKeyFactory.create("payments", "shared-listener", "key-1");

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void differentListenersAreIsolatedForTheSameDestinationAndKey() {
        EffectiveKey a = MessagingEffectiveKeyFactory.create("orders", "listener-a", "key-1");
        EffectiveKey b = MessagingEffectiveKeyFactory.create("orders", "listener-b", "key-1");

        assertThat(a).isNotEqualTo(b);
    }
}
