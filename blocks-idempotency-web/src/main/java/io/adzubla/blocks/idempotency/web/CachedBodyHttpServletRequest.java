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
 * <p>Nothing is read until {@link #getBody()} is first called. Until then
 * {@link #getInputStream()}/{@link #getReader()} hand out the container's own
 * stream, so a request the library never inspects (any handler that isn't
 * {@code @Idempotent}) is left exactly as it arrived - the container can
 * still parse a form or multipart body from it. Once {@link #getBody()} has
 * buffered the body, every later {@link #getInputStream()} call gets a fresh
 * stream over the buffered bytes.
 */
public class CachedBodyHttpServletRequest extends HttpServletRequestWrapper {

    private byte[] body;

    public CachedBodyHttpServletRequest(HttpServletRequest request) {
        super(request);
    }

    /** Reads (once) and returns the full request body; from here on the body is served from this buffer. */
    public byte[] getBody() {
        if (body == null) {
            try {
                body = super.getInputStream().readAllBytes();
            } catch (IOException e) {
                throw new UncheckedIOException("failed to buffer the request body", e);
            }
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
