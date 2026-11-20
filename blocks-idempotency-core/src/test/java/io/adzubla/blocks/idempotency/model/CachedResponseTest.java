package io.adzubla.blocks.idempotency.model;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CachedResponseTest {

    @Test
    void emptyHasNoBody() {
        assertThat(CachedResponse.empty().hasBody()).isFalse();
    }

    @Test
    void onlyTheSentinelIsEmpty() {
        assertThat(CachedResponse.empty().isEmpty()).isTrue();
        // A real response without a body (oversized, or no capture filter) carries its status.
        assertThat(new CachedResponse(201, Map.of(), null).isEmpty()).isFalse();
        assertThat(new CachedResponse(204, Map.of(), new byte[0]).isEmpty()).isFalse();
    }

    @Test
    void emptyHasNoHeaders() {
        assertThat(CachedResponse.empty().headers()).isEmpty();
    }

    @Test
    void aResponseWithABodyHasBody() {
        CachedResponse response = new CachedResponse(200, Map.of(), "body".getBytes());

        assertThat(response.hasBody()).isTrue();
    }
}
