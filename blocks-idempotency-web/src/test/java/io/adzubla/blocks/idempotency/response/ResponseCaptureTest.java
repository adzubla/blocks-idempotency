package io.adzubla.blocks.idempotency.response;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.util.unit.DataSize;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class ResponseCaptureTest {

    @Test
    void bodyWithinMaxSizeIsCaptured() throws Exception {
        ContentCachingResponseWrapper wrapper = wrapperWithBody(200, "{\"ok\":true}");

        CachedResponse captured = ResponseCapture.capture(wrapper, new String[0], DataSize.ofKilobytes(1));

        assertThat(captured.hasBody()).isTrue();
        assertThat(new String(captured.body(), StandardCharsets.UTF_8)).isEqualTo("{\"ok\":true}");
        assertThat(captured.status()).isEqualTo(200);
    }

    @Test
    void bodyOverMaxSizeIsDroppedButStatusAndHeadersAreStillCaptured() throws Exception {
        ContentCachingResponseWrapper wrapper = wrapperWithBody(201, "0123456789");
        wrapper.setHeader("X-Custom", "kept");

        CachedResponse captured = ResponseCapture.capture(wrapper, new String[0], DataSize.ofBytes(5));

        assertThat(captured.hasBody()).isFalse();
        assertThat(captured.body()).isNull();
        assertThat(captured.status()).isEqualTo(201);
        assertThat(captured.headers()).containsKey("X-Custom");
    }

    @Test
    void bodyExactlyAtMaxSizeIsStillCaptured() throws Exception {
        ContentCachingResponseWrapper wrapper = wrapperWithBody(200, "12345");

        CachedResponse captured = ResponseCapture.capture(wrapper, new String[0], DataSize.ofBytes(5));

        assertThat(captured.hasBody()).isTrue();
    }

    private static ContentCachingResponseWrapper wrapperWithBody(int status, String body) throws Exception {
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        ContentCachingResponseWrapper wrapper = new ContentCachingResponseWrapper(mockResponse);
        wrapper.setStatus(status);
        wrapper.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
        return wrapper;
    }
}
