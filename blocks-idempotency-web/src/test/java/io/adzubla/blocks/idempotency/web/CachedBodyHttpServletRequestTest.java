package io.adzubla.blocks.idempotency.web;

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The request-side size limit on the body the library buffers for fingerprinting and body-field
 * key resolution (issue 062): enforced while buffering, so an oversized body is rejected without
 * being read into memory in full.
 */
class CachedBodyHttpServletRequestTest {

    private static final int LIMIT = 16;

    @Test
    void aBodyWithinTheLimitIsBufferedAndStaysReadable() throws Exception {
        byte[] body = "{\"amount\":10}".getBytes(StandardCharsets.UTF_8);
        CachedBodyHttpServletRequest request = new CachedBodyHttpServletRequest(requestWith(body, false).request());

        assertThat(request.getBody(LIMIT)).isEqualTo(body);
        assertThat(request.getInputStream().readAllBytes()).isEqualTo(body);
    }

    @Test
    void aBodyExactlyAtTheLimitIsAccepted() {
        byte[] body = filled(LIMIT);

        assertThat(new CachedBodyHttpServletRequest(requestWith(body, false).request()).getBody(LIMIT)).isEqualTo(body);
    }

    @Test
    void aBodyOverTheLimitIsRejectedWithoutReadingTheWholeStream() {
        CountingRequest counting = requestWith(filled(10_000), false);
        CachedBodyHttpServletRequest request = new CachedBodyHttpServletRequest(counting.request());

        assertThatThrownBy(() -> request.getBody(LIMIT)).isInstanceOf(IdempotencyRequestTooLargeException.class);
        assertThat(counting.bytesRead()).isLessThanOrEqualTo(LIMIT + 1);
    }

    @Test
    void aDeclaredContentLengthOverTheLimitIsRejectedWithoutReadingAnything() {
        CountingRequest counting = requestWith(filled(10_000), true);
        CachedBodyHttpServletRequest request = new CachedBodyHttpServletRequest(counting.request());

        assertThatThrownBy(() -> request.getBody(LIMIT)).isInstanceOf(IdempotencyRequestTooLargeException.class);
        assertThat(counting.bytesRead()).isZero();
    }

    private static byte[] filled(int size) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte) 'a');
        return bytes;
    }

    /** A request whose input stream counts how many bytes were actually pulled from it. */
    private static CountingRequest requestWith(byte[] body, boolean declareContentLength) {
        MockHttpServletRequest mock = new MockHttpServletRequest("POST", "/orders");
        mock.setContentType("application/json");
        if (declareContentLength) {
            // MockHttpServletRequest derives Content-Length from its content; left unset, it
            // reports none (-1), like a chunked request. The body itself is served by the
            // counting stream below either way.
            mock.setContent(body);
        }
        int[] read = {0};
        ByteArrayInputStream source = new ByteArrayInputStream(body);
        ServletInputStream stream = new ServletInputStream() {
            @Override
            public boolean isFinished() {
                return source.available() == 0;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setReadListener(ReadListener readListener) {
            }

            @Override
            public int read() {
                int b = source.read();
                if (b >= 0) {
                    read[0]++;
                }
                return b;
            }

            @Override
            public int read(byte[] buffer, int offset, int length) {
                int n = source.read(buffer, offset, length);
                if (n > 0) {
                    read[0] += n;
                }
                return n;
            }
        };
        HttpServletRequest request = new HttpServletRequestWrapper(mock) {
            @Override
            public ServletInputStream getInputStream() {
                return stream;
            }
        };
        return new CountingRequest(request, read);
    }

    private record CountingRequest(HttpServletRequest request, int[] read) {
        int bytesRead() {
            return read[0];
        }
    }
}
