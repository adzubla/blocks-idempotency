package io.adzubla.blocks.idempotency.engine;

/**
 * Closed vocabulary for the {@code Idempotency-Reject-Reason} header, carried
 * on every non-2xx idempotency response (see ADR 0002 — "with the reason in a
 * header/body", extended to all non-2xx codes). {@link EngineDecision.Reject}
 * carries one of the first three, retryable ({@code 409 + Retry-After}).
 * {@link #RESPONSE_UNAVAILABLE} is the reason the caller reports for {@link
 * EngineDecision.Unavailable}, terminal ({@code 409} without {@code
 * Retry-After}) - note that decision is a zero-field record and doesn't
 * itself carry this value; the caller (see {@code IdempotencyInterceptor})
 * supplies it directly when handling that case. The remaining constants back
 * the {@code 400}/{@code 422}/{@code 503} exceptions, each fixed to a single
 * reason for its exception type - see {@code IdempotencyException#reason()}.
 */
public enum RejectReason {

    /** A concurrent duplicate found the key still in-progress. */
    IN_PROGRESS("in_progress"),
    /** The primary released the key (e.g. after an error) before this waiter observed it. */
    RELEASED("released"),
    /** WAIT mode gave up before the primary reached a terminal state. */
    TIMEOUT("timeout"),
    /** The effect completed but its response isn't replayable (crash window, or body over max-body-size). */
    RESPONSE_UNAVAILABLE("response_unavailable"),
    /** {@code keyRequired=true} but the header/body-field key was absent from the request. */
    KEY_REQUIRED("key_required"),
    /** The raw key value exceeds the configured size limit or contains characters outside the allowed charset. */
    KEY_INVALID("key_invalid"),
    /** The key was reused with a different payload (method+path+body fingerprint mismatch). */
    COLLISION("collision"),
    /** The store is unavailable and the resolved posture is {@code onStoreFailure=CLOSED}. */
    STORE_UNAVAILABLE("store_unavailable");

    /** The header this enum's {@link #wireValue()} is written to on a 409 response. */
    public static final String HEADER_NAME = "Idempotency-Reject-Reason";

    private final String wireValue;

    RejectReason(String wireValue) {
        this.wireValue = wireValue;
    }

    /** The value written to the reason header/body, per ADR 0002. */
    public String wireValue() {
        return wireValue;
    }
}
