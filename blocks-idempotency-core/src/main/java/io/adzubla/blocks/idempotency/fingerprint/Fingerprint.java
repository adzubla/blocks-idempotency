package io.adzubla.blocks.idempotency.fingerprint;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SHA-256 of method + path + the normalized (canonical key-ordered) request
 * body, used to detect a key reused with a different payload (see
 * {@code CONTEXT.md} — Collision).
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

    public static String sha256(String method, String path, byte[] body) {
        byte[] digest = digestBytes(
                method.getBytes(StandardCharsets.UTF_8),
                path.getBytes(StandardCharsets.UTF_8),
                normalize(body));
        return HexFormat.of().formatHex(digest);
    }

    /**
     * SHA-256 over {@code parts} joined with a {@code NUL} separator - the digest
     * shape shared by this class's own {@link #sha256} and by
     * {@code EffectiveKey.digestBytes()}.
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

    private static byte[] normalize(byte[] body) {
        if (body == null || body.length == 0) {
            return body == null ? new byte[0] : body;
        }
        try {
            Object tree = CANONICAL_MAPPER.readValue(body, Object.class);
            return CANONICAL_MAPPER.writeValueAsBytes(tree);
        } catch (JacksonException notJson) {
            return body;
        }
    }
}
