package io.adzubla.blocks.idempotency.web;

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Buffers the request body on demand so it can be read more than once: by the
 * library (fingerprint, body-field key extraction) and by the controller's
 * {@code @RequestBody}, independently and in any order.
 *
 * <p>Nothing is read until {@link #getBody(long)} is first called. Until then
 * {@link #getInputStream()}/{@link #getReader()} hand out the container's own
 * stream, so a request the library never inspects (any handler that isn't
 * {@code @Idempotent}) is left exactly as it arrived - the container can
 * still parse a form or multipart body from it. Once {@link #getBody(long)} has
 * buffered the body, every later {@link #getInputStream()} call gets a fresh
 * stream over the buffered bytes.
 */
public class CachedBodyHttpServletRequest extends HttpServletRequestWrapper {

    /**
     * The largest array the JVM reliably allocates. A body this size or larger can't be
     * buffered at all, so it's rejected whatever {@code maxBytes} is configured to.
     */
    private static final int MAX_BUFFERABLE_BYTES = Integer.MAX_VALUE - 8;

    private byte[] body;

    public CachedBodyHttpServletRequest(HttpServletRequest request) {
        super(request);
    }

    /**
     * Reads (once) and returns the full request body; from here on the body is served from this
     * buffer. A body over {@code maxBytes} is rejected without being read in full: immediately
     * when the declared {@code Content-Length} already exceeds it, otherwise as soon as one byte
     * past the limit has been read.
     *
     * @throws IdempotencyRequestTooLargeException if the body is larger than {@code maxBytes}
     */
    public byte[] getBody(long maxBytes) {
        if (body == null) {
            if (getContentLengthLong() > maxBytes) {
                throw new IdempotencyRequestTooLargeException();
            }
            // One byte past the limit is enough to know the body is too large.
            int readLimit = (int) Math.min(maxBytes + 1, MAX_BUFFERABLE_BYTES);
            byte[] read;
            try {
                read = super.getInputStream().readNBytes(readLimit);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to buffer the request body", e);
            }
            if (read.length > maxBytes || read.length == MAX_BUFFERABLE_BYTES) {
                throw new IdempotencyRequestTooLargeException();
            }
            body = read;
        }
        return body;
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        if (body == null) {
            return super.getInputStream();
        }
        ByteArrayInputStream byteStream = new ByteArrayInputStream(body);
        return new ServletInputStream() {
            @Override
            public boolean isFinished() {
                return byteStream.available() == 0;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setReadListener(ReadListener readListener) {
                // No async I/O support needed here.
            }

            @Override
            public int read() {
                return byteStream.read();
            }

            @Override
            public int read(byte[] buffer, int offset, int length) {
                return byteStream.read(buffer, offset, length);
            }
        };
    }

    @Override
    public BufferedReader getReader() throws IOException {
        if (body == null) {
            return super.getReader();
        }
        String encoding = getCharacterEncoding();
        Charset charset = encoding != null ? Charset.forName(encoding) : StandardCharsets.UTF_8;
        return new BufferedReader(new InputStreamReader(getInputStream(), charset));
    }
}
