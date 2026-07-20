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
    void emptyHasNoHeaders() {
        assertThat(CachedResponse.empty().headers()).isEmpty();
    }

    @Test
    void aResponseWithABodyHasBody() {
        CachedResponse response = new CachedResponse(200, Map.of(), "body".getBytes());

        assertThat(response.hasBody()).isTrue();
    }
}
