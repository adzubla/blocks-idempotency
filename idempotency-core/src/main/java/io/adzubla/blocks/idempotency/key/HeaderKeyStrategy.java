package io.adzubla.blocks.idempotency.key;

import jakarta.servlet.http.HttpServletRequest;

import java.util.Optional;

/**
 * Header strategy: the effective key's raw value comes from a client-supplied
 * HTTP header (see {@code CONTEXT.md} — Key Resolution Strategy).
 */
public final class HeaderKeyStrategy {

    private HeaderKeyStrategy() {
    }

    /** Resolves the raw key from {@code headerName}, absent if missing or blank. */
    public static Optional<String> resolve(HttpServletRequest request, String headerName) {
        String value = request.getHeader(headerName);
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
    }
}
