package io.adzubla.blocks.idempotency.key;

import com.jayway.jsonpath.JsonPath;
import io.adzubla.blocks.idempotency.model.Body;
import com.jayway.jsonpath.JsonPathException;

import java.math.BigDecimal;
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
        return resolve(Body.of(body), fieldPath);
    }

    /**
     * Same as {@link #resolve(byte[], String)}, reading the body's parsed tree
     * when it has one, so the same {@link Body} can then be fingerprinted
     * without parsing it again.
     *
     * <p>The key is identical to what reading the raw text gives: a body that
     * isn't strict JSON (the JSONPath parser is more lenient) and a
     * floating-point match (whose string form depends on the original digits)
     * are read from the raw text instead.
     *
     * @param body      the body
     * @param fieldPath a JSONPath expression, e.g. {@code $.orderId}
     * @return the key as a string, or empty if it cannot be resolved
     */
    public static Optional<String> resolve(Body body, String fieldPath) {
        if (body.bytes().length == 0) {
            return Optional.empty();
        }
        Optional<Object> tree = body.json();
        if (tree.isPresent()) {
            Object value;
            try {
                value = JsonPath.parse(tree.get()).read(fieldPath);
            } catch (JsonPathException e) {
                return Optional.empty();
            }
            if (!(value instanceof BigDecimal)) {
                return toScalarString(value);
            }
        }
        return resolveFromText(body.bytes(), fieldPath);
    }

    private static Optional<String> resolveFromText(byte[] body, String fieldPath) {
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
