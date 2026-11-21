package io.adzubla.blocks.idempotency.fingerprint;

import io.adzubla.blocks.idempotency.model.Body;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;

/**
 * SHA-256 of route + handler + the normalized (canonical key-ordered) request
 * body, used to detect a key reused with a different payload (see
 * {@code CONTEXT.md} — Collision). The {@code route} is whatever the caller
 * passes: a message listener passes its destination, and the HTTP interceptor
 * passes the request path plus the canonical query string, so the
 * same key with different query parameters is a collision, not a replay.
 *
 * <p>The body is parsed and re-serialized with object keys sorted
 * (recursively), so cosmetic field reordering doesn't change the
 * fingerprint. A non-JSON or empty body is hashed as-is.
 */
public final class Fingerprint {

    private static final JsonMapper CANONICAL_MAPPER = JsonMapper.builder()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();

    private Fingerprint() {
    }

    /**
     * Fingerprint of a request or delivery.
     *
     * @param route   the request path (plus canonical query) or message destination
     * @param handler the handler identity
     * @param body    the raw body; may be {@code null} or empty
     * @return the SHA-256 as lowercase hex
     */
    public static String sha256(String route, String handler, byte[] body) {
        return sha256(route, handler, Body.of(body));
    }

    /**
     * Fingerprint of a request or delivery whose body may already have been
     * parsed (for a body-field key): the parsed tree is reused, not re-parsed.
     *
     * @param route   the request path (plus canonical query) or message destination
     * @param handler the handler identity
     * @param body    the body
     * @return the SHA-256 as lowercase hex
     */
    public static String sha256(String route, String handler, Body body) {
        byte[] digest = digestBytes(
                route.getBytes(StandardCharsets.UTF_8),
                handler.getBytes(StandardCharsets.UTF_8),
                normalize(body));
        return HexFormat.of().formatHex(digest);
    }

    /**
     * SHA-256 over {@code parts} joined with a {@code NUL} separator - the digest
     * shape shared by this class's own {@link #sha256} and by
     * {@code EffectiveKey.digestBytes()}.
     *
     * @param parts the byte sequences to digest, in order
     * @return the 32-byte digest
     */
    public static byte[] digestBytes(byte[]... parts) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    digest.update((byte) 0);
                }
                digest.update(parts[i]);
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static byte[] normalize(Body body) {
        Optional<Object> tree = body.json();
        if (tree.isEmpty()) {
            return body.bytes();
        }
        try {
            return CANONICAL_MAPPER.writeValueAsBytes(tree.get());
        } catch (JacksonException unwritable) {
            return body.bytes();
        }
    }
}
