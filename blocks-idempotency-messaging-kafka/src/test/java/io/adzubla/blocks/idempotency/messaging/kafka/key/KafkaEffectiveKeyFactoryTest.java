package io.adzubla.blocks.idempotency.messaging.kafka.key;

import io.adzubla.blocks.idempotency.model.EffectiveKey;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaEffectiveKeyFactoryTest {

    @Test
    void scopesTheKeyByTopicListenerAndValueWithNoPrincipal() {
        EffectiveKey key = KafkaEffectiveKeyFactory.create("orders-topic", "orders-listener", "key-1");

        assertThat(key.route()).isEqualTo("orders-topic");
        assertThat(key.handler()).isEqualTo("orders-listener");
        assertThat(key.principal()).isEqualTo(EffectiveKey.NO_PRINCIPAL);
        assertThat(key.value()).isEqualTo("key-1");
    }

    @Test
    void differentTopicsAreIsolatedForTheSameListenerAndKey() {
        EffectiveKey a = KafkaEffectiveKeyFactory.create("orders-topic", "shared-listener", "key-1");
        EffectiveKey b = KafkaEffectiveKeyFactory.create("payments-topic", "shared-listener", "key-1");

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void differentListenersAreIsolatedForTheSameTopicAndKey() {
        EffectiveKey a = KafkaEffectiveKeyFactory.create("orders-topic", "listener-a", "key-1");
        EffectiveKey b = KafkaEffectiveKeyFactory.create("orders-topic", "listener-b", "key-1");

        assertThat(a).isNotEqualTo(b);
    }
}
