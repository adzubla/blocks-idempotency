package io.adzubla.blocks.idempotency.key;

import io.adzubla.blocks.idempotency.model.EffectiveKey;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EffectiveKeyFactoryTest {

    @Test
    void buildsTheEffectiveKeyFromItsRawComponents() {
        // Transport-neutral: no HttpServletRequest involved - e.g. a Kafka
        // listener would pass the destination/topic as route and the
        // listener id as handler directly.
        EffectiveKey key = EffectiveKeyFactory.create("orders-topic", "order-created-listener", EffectiveKey.NO_PRINCIPAL, "key-1");

        assertThat(key).isEqualTo(new EffectiveKey("orders-topic", "order-created-listener", EffectiveKey.NO_PRINCIPAL, "key-1"));
    }
}
