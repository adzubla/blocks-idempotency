package io.adzubla.blocks.idempotency.model;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A captured 2xx response, replayed verbatim on a repeat. Headers are already
 * filtered by the denylist ({@code Set-Cookie} always removed); {@code body} may be
 * {@code null} when the effect completed but its response is not replayable
 * (body over {@code max-body-size}, or the web capture filter wasn't in the chain).
 */
public record CachedResponse(int status, Map<String, List<String>> headers, byte[] body) {

    /** The status of the {@link #empty()} sentinel. No HTTP response has it, so a real response is never mistaken for the sentinel. */
    private static final int EMPTY_STATUS = 0;

    /**
     * Sentinel for "completed, nothing to cache" - used by a caller with no
     * response to capture (the Kafka/RabbitMQ/JMS messaging advice, which completes a
     * record purely to mark it done, not to replay anything). Since {@link
     * #isEmpty()} is true for it, {@code IdempotencyEngine#decisionForCompleted}
     * resolves a record completed with this to {@code EngineDecision.AlreadyProcessed}
     * - the routine "skip this duplicate" outcome (see ADR 0004) - and not to
     * {@code EngineDecision.Unavailable}, which is the HTTP error for a response that
     * should have been replayable but wasn't (oversized body, or no capture filter).
     * Those carry the real response status, so the two never look alike.
     */
    public static CachedResponse empty() {
        return new CachedResponse(EMPTY_STATUS, Map.of(), null);
    }

    /** Whether this is the {@link #empty()} sentinel: a record completed with no response to capture at all. */
    public boolean isEmpty() {
        return status == EMPTY_STATUS && body == null;
    }

    /** Whether a replayable body is present. */
    public boolean hasBody() {
        return body != null;
    }

    // Records auto-generate equals()/hashCode() using Objects.equals() per
    // component, which compares arrays by reference - two CachedResponse
    // instances holding equal but distinct byte[] (as happens once a store
    // round-trips the body, e.g. through a database) would never be equal
    // without these overrides.
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CachedResponse other)) {
            return false;
        }
        return status == other.status && Objects.equals(headers, other.headers) && Arrays.equals(body, other.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, headers, Arrays.hashCode(body));
    }
}
