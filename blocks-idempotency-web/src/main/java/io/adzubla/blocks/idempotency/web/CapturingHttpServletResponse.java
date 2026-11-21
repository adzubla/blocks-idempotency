package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.response.BoundedContentCachingResponseWrapper;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.IOException;

/**
 * A response wrapper that writes straight through until {@link
 * #startCapture(long)} is called, and from then on buffers the body (up to the
 * given limit) in a {@link BoundedContentCachingResponseWrapper} so it can be
 * captured for caching before {@link #copyBodyToResponse()} sends it to the
 * client.
 *
 * <p>{@link IdempotencyFilter} wraps every response before the handler is
 * known; only a request {@link IdempotencyInterceptor} actually reserves a
 * key for is ever buffered. Every other response - streaming, server-sent
 * events, large downloads, async results - is written exactly as it would be
 * without the library.
 */
public class CapturingHttpServletResponse extends HttpServletResponseWrapper {

    private volatile ContentCachingResponseWrapper capture;

    public CapturingHttpServletResponse(HttpServletResponse response) {
        super(response);
    }

    /** Starts buffering the whole body. Must be called before the handler obtains the output stream or writer. */
    public void startCapture() {
        startCapture(Long.MAX_VALUE);
    }

    /**
     * Starts buffering at most {@code limit} bytes; a larger body is sent through as it is
     * written, and not cached (see {@link BoundedContentCachingResponseWrapper}). Must be
     * called before the handler obtains the output stream or writer.
     */
    public void startCapture(long limit) {
        if (capture == null) {
            capture = new BoundedContentCachingResponseWrapper((HttpServletResponse) getResponse(), limit);
            setResponse(capture);
        }
    }

    /** Sends the buffered body, if any was captured, to the client. */
    public void copyBodyToResponse() throws IOException {
        ContentCachingResponseWrapper current = capture;
        if (current != null) {
            current.copyBodyToResponse();
        }
    }
}
