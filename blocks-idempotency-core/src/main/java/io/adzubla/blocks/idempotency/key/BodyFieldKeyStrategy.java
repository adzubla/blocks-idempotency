package io.adzubla.blocks.idempotency.key;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Body-field strategy: the effective key's raw value comes from a JSONPath
 * into the buffered raw request body (see {@code CONTEXT.md} — Key
 * Resolution Strategy). Represents business identity rather than the
 * client's own intent (contrast the web module's {@code HeaderKeyStrategy}).
 */
public final class BodyFieldKeyStrategy {

    private BodyFieldKeyStrategy() {
    }

    /**
     * Resolves the raw key from {@code fieldPath} over {@code body}, absent
     * when the path doesn't resolve to a scalar value: missing field,
     * malformed JSON, or a match that is itself a JSON object/array (a key must be a
     * scalar identity, not a compound structure). A number or boolean match is
     * coerced to its string form.
     *
     * @param body      the raw request or message body; may be {@code null} or empty
     * @param fieldPath a JSONPath expression, e.g. {@code $.orderId}
     * @return the key as a string, or empty if it cannot be resolved
     */
    public static Optional<String> resolve(byte[] body, String fieldPath) {
        if (body == null || body.length == 0) {
            return Optional.empty();
        }
        Object value;
        try {
            value = JsonPath.read(new String(body, StandardCharsets.UTF_8), fieldPath);
        } catch (JsonPathException e) {
            return Optional.empty();
        }
        return toScalarString(value);
    }

    private static Optional<String> toScalarString(Object value) {
        if (value instanceof String s) {
            return s.isBlank() ? Optional.empty() : Optional.of(s);
        }
        if (value instanceof Number || value instanceof Boolean) {
            return Optional.of(value.toString());
        }
        // null, or a non-scalar match (Map/List) - no key found.
        return Optional.empty();
    }
}
