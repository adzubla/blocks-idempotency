package io.adzubla.blocks.idempotency.response;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.web.util.ContentCachingResponseWrapper;
import org.springframework.web.util.WebUtils;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;

/**
 * A {@link ContentCachingResponseWrapper} that buffers at most {@code limit}
 * bytes. Past that point the body can't be cached anyway (it is over {@code
 * idempotency.max-body-size}), so holding it in memory would cost heap for
 * nothing: the wrapper sends what it has buffered to the client, then writes
 * the rest straight through, and reports {@link #isOverflowed()} so the
 * captured response is recorded without a body ({@code response_unavailable}).
 *
 * <p>A declared {@code Content-Length} or {@code setBufferSize} over the limit
 * switches to pass-through immediately, so no buffer of that size is
 * allocated. Once overflowed, the response is committed as it is written, so a
 * later error can no longer replace it; that is why a store whose {@code
 * complete()} may still roll the effect back is given no limit at all.
 */
public class BoundedContentCachingResponseWrapper extends ContentCachingResponseWrapper {

    private final long limit;
    private boolean overflowed;
    private ServletOutputStream outputStream;
    private PrintWriter writer;

    /**
     * @param response the response to wrap
     * @param limit    the most bytes to buffer; {@link Long#MAX_VALUE} buffers everything
     */
    public BoundedContentCachingResponseWrapper(HttpServletResponse response, long limit) {
        super(response);
        this.limit = limit;
    }

    /** @return whether the body outgrew the limit and was sent through without being kept */
    public boolean isOverflowed() {
        return overflowed;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        if (outputStream == null) {
            outputStream = new LimitedOutputStream(super.getOutputStream());
        }
        return outputStream;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
        if (writer == null) {
            String encoding = getCharacterEncoding();
            Charset charset = Charset.forName(encoding != null ? encoding : WebUtils.DEFAULT_CHARACTER_ENCODING);
            writer = new FlushingPrintWriter(new OutputStreamWriter(getOutputStream(), charset));
        }
        return writer;
    }

    @Override
    public void flushBuffer() throws IOException {
        if (overflowed) {
            getResponse().flushBuffer();
        }
    }

    @Override
    public void setBufferSize(int size) {
        if (size > limit) {
            getResponse().setBufferSize(size);
        } else {
            super.setBufferSize(size);
        }
    }

    @Override
    public void setContentLength(int len) {
        setContentLengthLong(len);
    }

    @Override
    public void setContentLengthLong(long len) {
        if (overflowed || len > limit) {
            overflow();
            getResponse().setContentLengthLong(len);
        } else if (len > Integer.MAX_VALUE) {
            super.setContentLengthLong(len); // rejects it, as the parent always has
        } else {
            // Not super.setContentLengthLong: the parent implements it by calling
            // setContentLength(int), which is overridden here to come back to this method.
            super.setContentLength((int) len);
        }
    }

    @Override
    public void setHeader(String name, String value) {
        if (isContentLength(name) && value != null) {
            setContentLengthLong(Long.parseLong(value));
        } else {
            super.setHeader(name, value);
        }
    }

    @Override
    public void addHeader(String name, String value) {
        if (isContentLength(name)) {
            setContentLengthLong(Long.parseLong(value));
        } else {
            super.addHeader(name, value);
        }
    }

    @Override
    public void setIntHeader(String name, int value) {
        if (isContentLength(name)) {
            setContentLengthLong(value);
        } else {
            super.setIntHeader(name, value);
        }
    }

    @Override
    public void addIntHeader(String name, int value) {
        if (isContentLength(name)) {
            setContentLengthLong(value);
        } else {
            super.addIntHeader(name, value);
        }
    }

    @Override
    public void reset() {
        super.reset();
        overflowed = false;
    }

    private static boolean isContentLength(String name) {
        return HttpHeaders.CONTENT_LENGTH.equalsIgnoreCase(name);
    }

    /**
     * Sends what has been buffered, with any {@code Content-Length} declared so far, and
     * switches to writing straight through.
     */
    private void overflow() {
        if (overflowed) {
            return;
        }
        overflowed = true;
        try {
            String declaredLength = super.getHeader(HttpHeaders.CONTENT_LENGTH);
            copyBodyToResponse(false);
            HttpServletResponse raw = (HttpServletResponse) getResponse();
            if (declaredLength != null && !raw.isCommitted() && raw.getHeader(HttpHeaders.CONTENT_LENGTH) == null) {
                raw.setContentLengthLong(Long.parseLong(declaredLength));
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("failed to send the buffered response body", e);
        }
    }

    /** Buffers through the parent's caching stream until the limit, then writes to the real response. */
    private final class LimitedOutputStream extends ServletOutputStream {

        private final ServletOutputStream caching;

        LimitedOutputStream(ServletOutputStream caching) {
            this.caching = caching;
        }

        @Override
        public void write(int b) throws IOException {
            target(1).write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            target(len).write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            if (overflowed) {
                getResponse().getOutputStream().flush();
            }
        }

        private ServletOutputStream target(int len) throws IOException {
            if (!overflowed && getContentSize() + (long) len > limit) {
                overflow();
            }
            return overflowed ? getResponse().getOutputStream() : caching;
        }

        @Override
        public boolean isReady() {
            return caching.isReady();
        }

        @Override
        public void setWriteListener(WriteListener listener) {
            caching.setWriteListener(listener);
        }
    }

    /** Flushes after every write, so the stream (and the limit check) always sees the bytes written so far. */
    private static final class FlushingPrintWriter extends PrintWriter {

        FlushingPrintWriter(OutputStreamWriter out) {
            super(out);
        }

        @Override
        public void write(char[] buf, int off, int len) {
            super.write(buf, off, len);
            super.flush();
        }

        @Override
        public void write(String s, int off, int len) {
            super.write(s, off, len);
            super.flush();
        }

        @Override
        public void write(int c) {
            super.write(c);
            super.flush();
        }
    }
}
