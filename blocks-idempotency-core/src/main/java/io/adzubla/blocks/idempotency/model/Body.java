package io.adzubla.blocks.idempotency.model;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.Optional;

/**
 * The raw body of a request or delivery, parsed as JSON at most once. The
 * body-field key strategy and the fingerprint both need the parsed form, so
 * sharing one {@code Body} between them halves the parsing work for a
 * body-field key.
 *
 * <p>The parsed form is a plain tree of {@code Map}/{@code List}/scalars, with
 * floating-point numbers as {@code BigDecimal} so the fingerprint keeps their
 * exact digits. A body that is empty or not valid JSON has no parsed form.
 * Not thread-safe; use one instance per request or delivery.
 */
public final class Body {

    private static final byte[] EMPTY = new byte[0];

    private static final JsonMapper PARSER = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();

    private final byte[] bytes;
    private boolean parsed;
    private Object json;

    private Body(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * @param bytes the raw body; {@code null} is treated as empty
     * @return a body that parses {@code bytes} lazily, on first use
     */
    public static Body of(byte[] bytes) {
        return new Body(bytes == null ? EMPTY : bytes);
    }

    /** @return the raw bytes, never {@code null} */
    public byte[] bytes() {
        return bytes;
    }

    /** @return the parsed JSON tree, or empty if the body is empty or not valid JSON */
    public Optional<Object> json() {
        if (!parsed) {
            parsed = true;
            if (bytes.length > 0) {
                try {
                    json = PARSER.readValue(bytes, Object.class);
                } catch (JacksonException notJson) {
                    json = null;
                }
            }
        }
        return Optional.ofNullable(json);
    }
}
