package io.adzubla.blocks.idempotency.web.key;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

class HeaderKeyStrategyTest {

    @Test
    void resolvesThePresentHeader() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(Idempotent.IDEMPOTENCY_KEY_HEADER, "abc-123");

        assertThat(HeaderKeyStrategy.resolve(request, Idempotent.IDEMPOTENCY_KEY_HEADER)).contains("abc-123");
    }

    @Test
    void isEmptyWhenHeaderIsAbsent() {
        MockHttpServletRequest request = new MockHttpServletRequest();

        assertThat(HeaderKeyStrategy.resolve(request, Idempotent.IDEMPOTENCY_KEY_HEADER)).isEmpty();
    }

    @Test
    void isEmptyWhenHeaderIsBlank() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(Idempotent.IDEMPOTENCY_KEY_HEADER, "   ");

        assertThat(HeaderKeyStrategy.resolve(request, Idempotent.IDEMPOTENCY_KEY_HEADER)).isEmpty();
    }
}
