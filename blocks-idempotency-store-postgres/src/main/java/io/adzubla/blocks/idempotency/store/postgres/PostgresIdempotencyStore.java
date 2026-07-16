package io.adzubla.blocks.idempotency.store.postgres;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.IdempotencyRecord;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.StoreUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Exactly-once {@link IdempotencyStore} backed by PostgreSQL (ADR 0001, ADR 0003).
 *
 * <p>{@code reserve()} opens the transaction the effect will run in and, when it
 * wins the reservation, leaves it open and thread-bound (Spring's ordinary
 * resource binding) rather than committing - a handler's own {@code @Transactional}
 * (default {@code REQUIRED} propagation), or even plain unannotated JDBC/JPA calls
 * on the same thread, transparently join it. {@code complete()} writes the response
 * and commits; {@code release()} just rolls back - undoing the reservation
 * {@code INSERT} along with anything the handler wrote, no {@code DELETE} needed.
 * Both are fenced by {@code reservation_token} against a superseded reservation.
 *
 * <p>There is deliberately no {@code lock-ttl} enforcement via a background sweep
 * here: concurrency is native (a second reservation attempt blocks on the unique
 * index until the first commits or rolls back). {@code expires_at} is still
 * maintained so a reservation whose owner never called {@code complete}/{@code
 * release} - and whose transaction is therefore still sitting open - can be
 * superseded once real time has passed it, via the {@code ON CONFLICT ... DO
 * UPDATE ... WHERE} clause below (evaluated with {@code clock_timestamp()}, not
 * {@code now()}, since {@code now()} is frozen for the lifetime of a transaction).
 * A bare {@code DO NOTHING} (ADR 0001/0003's original sketch) can't express this
 * supersession - it silently no-ops on any conflict, stale or not - so the
 * shared {@code IdempotencyStoreContractTest}'s "past its lock-ttl" test needs
 * the conditional {@code DO UPDATE} instead. See ADR 0003's consequences.
 *
 * <p>The blocking wait above is bounded by {@code lock_timeout} (ADR
 * 0001/0002/Slice 018): {@code reserve()} sets it for its own transaction
 * before attempting the reservation, so a waiter gives up rather than holding
 * its thread indefinitely if the row's owner neither commits nor rolls back in
 * time. A lock-timeout error is fenced through as an ordinary {@code
 * IN_PROGRESS} record carrying the caller's own fingerprint, so the engine's
 * existing {@code RejectReason.IN_PROGRESS}/WAIT handling applies unchanged.
 *
 * <p>Not yet supported: async request handling - {@code Callable}/{@code
 * DeferredResult}/WebFlux-style handlers can hop threads between {@code
 * reserve()} and the handler body, which breaks the thread-bound transaction
 * this store relies on (ADR 0003's consequences).
 */
public class PostgresIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresIdempotencyStore.class);

    /** Bean qualifier used by {@code @Idempotent(store = ...)}. */
    public static final String QUALIFIER = "postgres";

    private static final String UPSERT_RESERVATION = """
            INSERT INTO idempotency_record
                (http_method, path, principal, idempotency_key, fingerprint, reservation_token, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, clock_timestamp(), clock_timestamp() + ?::interval)
            ON CONFLICT (http_method, path, principal, idempotency_key) DO UPDATE SET
                fingerprint = EXCLUDED.fingerprint,
                reservation_token = EXCLUDED.reservation_token,
                response_status = NULL,
                response_headers = NULL,
                response_body = NULL,
                created_at = EXCLUDED.created_at,
                expires_at = EXCLUDED.expires_at
            WHERE idempotency_record.expires_at < clock_timestamp()
            RETURNING http_method
            """;

    private static final String SELECT_RECORD = """
            SELECT fingerprint, response_status, response_headers, response_body
            FROM idempotency_record
            WHERE http_method = ? AND path = ? AND principal = ? AND idempotency_key = ? AND expires_at > clock_timestamp()
            """;

    private static final String COMPLETE = """
            UPDATE idempotency_record
            SET response_status = ?, response_headers = ?::jsonb, response_body = ?, expires_at = clock_timestamp() + ?::interval
            WHERE http_method = ? AND path = ? AND principal = ? AND idempotency_key = ? AND reservation_token = ?
            """;

    /** SQLState 55P03 ("lock_not_available") - raised when {@code lock_timeout} is exceeded. */
    private static final String SQLSTATE_LOCK_NOT_AVAILABLE = "55P03";

    private final JdbcTemplate jdbc;
    private final PlatformTransactionManager transactionManager;
    private final ObjectMapper headerMapper;
    private final Duration lockTimeout;
    private final ThreadLocal<ReservationScope> scope = new ThreadLocal<>();

    public PostgresIdempotencyStore(JdbcTemplate jdbc, PlatformTransactionManager transactionManager, ObjectMapper headerMapper,
            Duration lockTimeout) {
        this.jdbc = jdbc;
        this.transactionManager = transactionManager;
        this.headerMapper = headerMapper;
        this.lockTimeout = lockTimeout;
    }

    @Override
    public ReservationResult reserve(EffectiveKey key, String fingerprint, Duration lockTtl) {
        TransactionStatus status;
        try {
            status = transactionManager.getTransaction(new DefaultTransactionDefinition());
        } catch (RuntimeException e) {
            throw asStoreUnavailableIfConnectionFailure(e);
        }
        String fenceToken = UUID.randomUUID().toString();
        try {
            // String-built, not `?`-bound like every other statement here:
            // Postgres's SET doesn't accept a bind parameter in the value
            // position. Safe regardless - lockTimeout is trusted config, never
            // user input.
            jdbc.execute("SET LOCAL lock_timeout = '" + lockTimeout.toMillis() + "ms'");
            Object[] args = concat(keyArgs(key), new Object[] {fingerprint, fenceToken, toPgInterval(lockTtl)});
            boolean won = !jdbc.query(UPSERT_RESERVATION, (rs, rowNum) -> rs.getString(1), args).isEmpty();
            if (won) {
                bindScope(status, fenceToken);
                return ReservationResult.reserved(fenceToken);
            }

            // Lost the conflict: either a genuinely different, still-valid
            // record exists, or (nested call on a thread with an already-open
            // scope) it's our own not-yet-superseded reservation.
            IdempotencyRecord existing = findRecord(key).orElseThrow(() ->
                    new IllegalStateException("reservation conflict but no record visible for " + key));
            transactionManager.commit(status);
            return ReservationResult.exists(existing);
        } catch (RuntimeException e) {
            if (isLockTimeout(e)) {
                // Gave up waiting on the row's lock before its owner committed
                // or rolled back (ADR 0001/0002's bounded native block) - still
                // genuinely in-progress from here. Fencing this through as an
                // IN_PROGRESS record carrying the caller's own fingerprint (we
                // can't safely read the real one without blocking further)
                // reuses the engine's existing RejectReason.IN_PROGRESS/WAIT
                // handling unchanged (ADR 0003's consequences). Accepted
                // tradeoff: a caller whose body genuinely differs from the
                // primary's won't see a 422 collision in this narrow window -
                // it degrades to a 409/WAIT like any other in-progress
                // conflict, resolved for real once the row's lock frees up.
                log.debug("Postgres reservation lock timeout - treating as in-progress: {}", key);
                safeRollback(status);
                return ReservationResult.exists(new IdempotencyRecord(RecordState.IN_PROGRESS, fingerprint, null));
            }
            safeRollback(status);
            throw asStoreUnavailableIfConnectionFailure(e);
        }
    }

    private static boolean isLockTimeout(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sqlException && SQLSTATE_LOCK_NOT_AVAILABLE.equals(sqlException.getSQLState())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<IdempotencyRecord> find(EffectiveKey key) {
        try {
            return findRecord(key);
        } catch (RuntimeException e) {
            throw asStoreUnavailableIfConnectionFailure(e);
        }
    }

    @Override
    public void complete(EffectiveKey key, String fenceToken, CachedResponse response, Duration responseTtl) {
        ReservationScope current = scope.get();
        if (current == null || !current.fenceToken().equals(fenceToken)) {
            // Fenced off: this token no longer owns the reservation (it was
            // superseded). The real owner's scope, if any, is left untouched.
            return;
        }
        try {
            Object[] args = concat(new Object[] {response.status(), toJson(response.headers()), response.body(), toPgInterval(responseTtl)},
                    keyArgs(key), new Object[] {fenceToken});
            jdbc.update(COMPLETE, args);
            transactionManager.commit(current.status());
        } catch (RuntimeException e) {
            safeRollback(current.status());
            throw asStoreUnavailableIfConnectionFailure(e);
        } finally {
            scope.remove();
        }
    }

    @Override
    public void release(EffectiveKey key, String fenceToken) {
        ReservationScope current = scope.get();
        if (current == null || !current.fenceToken().equals(fenceToken)) {
            return;
        }
        try {
            // Rollback alone undoes the reservation INSERT/UPDATE (and
            // anything the handler wrote in the same transaction) - no DELETE
            // needed (ADR 0003).
            transactionManager.rollback(current.status());
        } catch (RuntimeException e) {
            throw asStoreUnavailableIfConnectionFailure(e);
        } finally {
            scope.remove();
        }
    }

    /**
     * Best-effort rollback for the path where we've already decided the
     * outcome from the primary failure ({@code e} above) - if the connection
     * is broken, rollback fails too, and that secondary failure must not
     * replace the classification already made. The pool discards a broken
     * connection regardless of whether rollback "succeeds".
     */
    private void safeRollback(TransactionStatus status) {
        try {
            transactionManager.rollback(status);
        } catch (RuntimeException ignored) {
            // See method javadoc - intentionally swallowed.
        }
    }

    /**
     * Translates a genuine connection-level failure (SQL statement,
     * transaction start, or commit/rollback) into {@link StoreUnavailableException};
     * anything else (a real bug - a Lua-script-equivalent mistake, a
     * constraint violation) passes through unchanged so it isn't masked as an
     * outage.
     */
    private static RuntimeException asStoreUnavailableIfConnectionFailure(RuntimeException e) {
        boolean connectionFailure = e instanceof DataAccessResourceFailureException
                || e instanceof QueryTimeoutException
                || e instanceof CannotCreateTransactionException
                || e instanceof TransactionSystemException;
        if (connectionFailure) {
            log.warn("Postgres idempotency store unavailable", e);
            return new StoreUnavailableException("Postgres is unavailable", e);
        }
        return e;
    }

    /**
     * Test-only escape hatch: rolls back and clears a reservation left
     * dangling on the current thread because a test never called {@code
     * complete}/{@code release} (simulating a crash - the one thing
     * {@code Thread.sleep} in the shared contract test suite can't
     * literally reproduce, since it doesn't close the connection). Production
     * code always resolves via {@link #complete}/{@link #release}, driven by
     * {@code IdempotencyInterceptor#afterCompletion}, which is guaranteed to
     * run.
     */
    void abandonDanglingReservationForTests() {
        ReservationScope current = scope.get();
        if (current != null) {
            transactionManager.rollback(current.status());
            scope.remove();
        }
    }

    /**
     * Records the outer, commit/rollback-owning {@link TransactionStatus} for
     * the reservation now held by this thread, alongside the fence token that
     * currently owns it. A nested {@code reserve()} call (the contract test's
     * same-thread self-supersede case) must not replace the outer status with
     * its own merely-participating one - only the token moves.
     */
    private void bindScope(TransactionStatus status, String fenceToken) {
        ReservationScope existing = scope.get();
        scope.set(existing == null ? new ReservationScope(status, fenceToken) : new ReservationScope(existing.status(), fenceToken));
    }

    private Optional<IdempotencyRecord> findRecord(EffectiveKey key) {
        return jdbc.query(SELECT_RECORD, this::mapRecord, keyArgs(key)).stream().findFirst();
    }

    private static Object[] keyArgs(EffectiveKey key) {
        return new Object[] {key.method(), key.path(), key.principal(), key.value()};
    }

    private static Object[] concat(Object[]... parts) {
        int total = 0;
        for (Object[] part : parts) {
            total += part.length;
        }
        Object[] result = new Object[total];
        int pos = 0;
        for (Object[] part : parts) {
            System.arraycopy(part, 0, result, pos, part.length);
            pos += part.length;
        }
        return result;
    }

    private IdempotencyRecord mapRecord(ResultSet rs, int rowNum) throws SQLException {
        String fingerprint = rs.getString("fingerprint");
        int status = rs.getInt("response_status");
        if (rs.wasNull()) {
            return new IdempotencyRecord(RecordState.IN_PROGRESS, fingerprint, null);
        }
        byte[] body = rs.getBytes("response_body");
        Map<String, List<String>> headers = parseHeaders(rs.getString("response_headers"));
        return new IdempotencyRecord(RecordState.COMPLETED, fingerprint, new CachedResponse(status, headers, body));
    }

    private static String toPgInterval(Duration duration) {
        return duration.toMillis() + " milliseconds";
    }

    private String toJson(Map<String, List<String>> headers) {
        try {
            return headerMapper.writeValueAsString(headers);
        } catch (JacksonException e) {
            throw new IllegalStateException("failed to serialize response headers", e);
        }
    }

    private Map<String, List<String>> parseHeaders(String json) {
        if (json == null) {
            return Map.of();
        }
        try {
            return headerMapper.readValue(json, new TypeReference<>() {
            });
        } catch (JacksonException e) {
            throw new IllegalStateException("failed to parse response headers", e);
        }
    }

    private record ReservationScope(TransactionStatus status, String fenceToken) {
    }
}
