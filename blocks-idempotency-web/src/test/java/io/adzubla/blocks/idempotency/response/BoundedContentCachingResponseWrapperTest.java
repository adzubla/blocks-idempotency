package io.adzubla.blocks.idempotency.response;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedContentCachingResponseWrapperTest {

    private final MockHttpServletResponse raw = new MockHttpServletResponse();

    @Test
    void aBodyWithinTheLimitIsBufferedAndOnlySentOnCopy() throws Exception {
        BoundedContentCachingResponseWrapper wrapper = new BoundedContentCachingResponseWrapper(raw, 10);

        wrapper.getOutputStream().write(bytes("0123456789"));

        assertThat(wrapper.isOverflowed()).isFalse();
        assertThat(wrapper.getContentAsByteArray()).isEqualTo(bytes("0123456789"));
        assertThat(raw.getContentAsByteArray()).isEmpty();

        wrapper.copyBodyToResponse();
        assertThat(raw.getContentAsString()).isEqualTo("0123456789");
        assertThat(raw.getContentLength()).isEqualTo(10);
    }

    @Test
    void aBodyOverTheLimitIsSentThroughInOrderAndNotKept() throws Exception {
        BoundedContentCachingResponseWrapper wrapper = new BoundedContentCachingResponseWrapper(raw, 10);

        wrapper.getOutputStream().write(bytes("01234"));
        wrapper.getOutputStream().write(bytes("56789ABCDE"));
        wrapper.getOutputStream().write('F');
        wrapper.copyBodyToResponse();

        assertThat(wrapper.isOverflowed()).isTrue();
        assertThat(wrapper.getContentAsByteArray()).isEmpty();
        assertThat(raw.getContentAsString()).isEqualTo("0123456789ABCDEF");
    }

    @Test
    void theWriterIsBoundedToo() throws Exception {
        BoundedContentCachingResponseWrapper wrapper = new BoundedContentCachingResponseWrapper(raw, 4);
        wrapper.setCharacterEncoding("UTF-8");

        PrintWriter writer = wrapper.getWriter();
        writer.write("ab");
        assertThat(wrapper.getContentAsByteArray()).isEqualTo(bytes("ab"));
        writer.write("cdef");
        wrapper.copyBodyToResponse();

        assertThat(wrapper.isOverflowed()).isTrue();
        assertThat(raw.getContentAsString()).isEqualTo("abcdef");
    }

    @Test
    void aDeclaredContentLengthOverTheLimitPassesThroughWithoutBuffering() throws Exception {
        BoundedContentCachingResponseWrapper wrapper = new BoundedContentCachingResponseWrapper(raw, 4);

        wrapper.setContentLength(6);
        assertThat(wrapper.isOverflowed()).isTrue();
        assertThat(raw.getContentLength()).isEqualTo(6);

        wrapper.getOutputStream().write(bytes("abcdef"));
        assertThat(raw.getContentAsString()).isEqualTo("abcdef");
    }

    @Test
    void aContentLengthDeclaredWithinTheLimitIsKeptWhenTheBodyOverflowsAnyway() throws Exception {
        BoundedContentCachingResponseWrapper wrapper = new BoundedContentCachingResponseWrapper(raw, 4);

        wrapper.setHeader("Content-Length", "3");
        wrapper.getOutputStream().write(bytes("abcdef"));

        assertThat(wrapper.isOverflowed()).isTrue();
        assertThat(raw.getContentLength()).isEqualTo(3);
    }

    @Test
    void anUnboundedWrapperBuffersEverything() throws Exception {
        BoundedContentCachingResponseWrapper wrapper = new BoundedContentCachingResponseWrapper(raw, Long.MAX_VALUE);

        wrapper.setContentLength(100_000);
        wrapper.getOutputStream().write(new byte[100_000]);

        assertThat(wrapper.isOverflowed()).isFalse();
        assertThat(wrapper.getContentSize()).isEqualTo(100_000);
        assertThat(raw.getContentAsByteArray()).isEmpty();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
