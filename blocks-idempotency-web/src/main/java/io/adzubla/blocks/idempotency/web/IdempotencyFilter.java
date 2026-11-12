package io.adzubla.blocks.idempotency.web;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.util.WebUtils;

import java.io.IOException;

/**
 * Wraps request and response for buffered reads/writes: the request so its
 * body survives multiple reads (see {@link CachedBodyHttpServletRequest}), the
 * response so its body can be captured for caching before being sent to the
 * client (see {@link CapturingHttpServletResponse}).
 *
 * <p>Runs in front of every request, before the handler is known, so both
 * wrappers are passive: nothing is read or buffered until {@link
 * IdempotencyInterceptor} asks for it on behalf of an {@code @Idempotent}
 * handler. Every other request and response passes through untouched.
 *
 * <p>Registered for the {@code REQUEST} and {@code ASYNC} dispatches. A
 * handler that completes asynchronously leaves this filter on the first
 * dispatch with nothing written yet; its result is written on a later async
 * dispatch, which arrives here carrying the wrappers from the first one. The
 * buffered body is therefore sent only once the request is no longer async.
 */
public class IdempotencyFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (!(request instanceof HttpServletRequest httpRequest) || !(response instanceof HttpServletResponse httpResponse)) {
            chain.doFilter(request, response);
            return;
        }

        CapturingHttpServletResponse alreadyWrapped = WebUtils.getNativeResponse(httpResponse, CapturingHttpServletResponse.class);
        if (alreadyWrapped != null) {
            // An async dispatch of a request this filter wrapped on its first dispatch.
            doFilter(httpRequest, httpRequest, httpResponse, alreadyWrapped, chain);
            return;
        }
        doFilter(httpRequest, new CachedBodyHttpServletRequest(httpRequest), null, new CapturingHttpServletResponse(httpResponse), chain);
    }

    private static void doFilter(HttpServletRequest original, HttpServletRequest requestToUse, HttpServletResponse responseToUse,
            CapturingHttpServletResponse capturing, FilterChain chain) throws IOException, ServletException {
        try {
            chain.doFilter(requestToUse, responseToUse != null ? responseToUse : capturing);
        } finally {
            if (!original.isAsyncStarted()) {
                capturing.copyBodyToResponse();
            }
        }
    }
}
