package io.adzubla.blocks.idempotency.key;

import java.util.regex.Pattern;

/**
 * The opaque-string contract a raw key value must satisfy (see {@code
 * CONTEXT.md} - "Key format"): a bounded length ({@code
 * idempotency.key.max-length}) and a fixed charset. The charset is
 * intentionally not configurable - one hardcoded pattern is enough to keep
 * keys safe as HTTP header values and store-key components; ULID is a
 * recommended client convention but any value matching this charset is
 * accepted.
 */
public final class KeyFormat {

    private static final Pattern CHARSET = Pattern.compile("[A-Za-z0-9_.:-]+");

    private KeyFormat() {
    }

    public static boolean isValid(String rawKey, int maxLength) {
        return rawKey.length() <= maxLength && CHARSET.matcher(rawKey).matches();
    }
}
