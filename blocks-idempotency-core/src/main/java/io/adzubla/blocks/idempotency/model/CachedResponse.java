package io.adzubla.blocks.idempotency.model;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A captured 2xx response, replayed verbatim on a repeat. Headers are already
 * filtered by the denylist ({@code Set-Cookie} always removed); {@code body} may be
 * {@code null} when the effect completed but its response is not replayable
 * (crash window, or body over {@code max-body-size}).
 */
public record CachedResponse(int status, Map<String, List<String>> headers, byte[] body) {

    /**
     * Sentinel for "completed, nothing to cache" - used by a caller with no
     * response to capture (e.g. a future messaging adapter, which completes a
     * record purely to mark it done, not to replay anything). Since {@link
     * #hasBody()} is false, {@code IdempotencyEngine#decisionForCompleted}
     * resolves a record completed with this to {@code EngineDecision.Unavailable}
     * - for HTTP that decision means a terminal error (crash window/oversized
     * body), but a caller that always completes with this sentinel will see
     * {@code Unavailable} as its routine, expected outcome instead (see ADR
     * 0004), not an error to surface to the end user.
     */
    public static CachedResponse empty() {
        return new CachedResponse(0, Map.of(), null);
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
