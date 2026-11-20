package io.adzubla.blocks.idempotency.store.postgres;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.IdempotencyRecord;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import io.adzubla.blocks.idempotency.store.EffectRolledBackException;
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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HexFormat;
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
 * <p>{@code await()} (ADR 0002 WAIT mode) overrides {@link IdempotencyStore}'s
 * polling default with the same native primitive {@code reserve()} uses -
 * conflict detection on {@code INSERT ... ON CONFLICT} blocks on the row and
 * unblocks the instant the primary resolves, rather than polling {@code
 * find()} on an interval. A bare {@code SELECT ... FOR UPDATE} can't do this:
 * an in-flight, uncommitted {@code INSERT} is invisible to every other
 * transaction under MVCC, so it would see nothing and return instantly
 * rather than wait.
 *
 * <p>Not supported: async request handling - {@code Callable}/{@code
 * DeferredResult}/WebFlux-style handlers can hop threads between {@code
 * reserve()} and the handler body, which breaks the thread-bound transaction
 * this store relies on (ADR 0003's consequences). {@link #supportsAsyncHandlers()}
 * reports this, so startup validation rejects such handlers.
 */
public class PostgresIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresIdempotencyStore.class);

    /** Bean qualifier used by {@code @Idempotent(store = ...)}. */
    public static final String QUALIFIER = "postgres";

    private static final String UPSERT_RESERVATION = """
            INSERT INTO idempotency_record
                (route, handler, principal, idempotency_key, fingerprint, reservation_token, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, clock_timestamp(), clock_timestamp() + ?::interval)
            ON CONFLICT (route, handler, principal, idempotency_key) DO UPDATE SET
                fingerprint = EXCLUDED.fingerprint,
                reservation_token = EXCLUDED.reservation_token,
                response_status = NULL,
                response_headers = NULL,
                response_body = NULL,
                created_at = EXCLUDED.created_at,
                expires_at = EXCLUDED.expires_at
            WHERE idempotency_record.expires_at < clock_timestamp()
            RETURNING route
            """;

    private static final String SELECT_RECORD = """
            SELECT fingerprint, response_status, response_headers, response_body
            FROM idempotency_record
            WHERE route = ? AND handler = ? AND principal = ? AND idempotency_key = ? AND expires_at > clock_timestamp()
            """;

    private static final String COMPLETE = """
            UPDATE idempotency_record
            SET response_status = ?, response_headers = ?::jsonb, response_body = ?, expires_at = clock_timestamp() + ?::interval
            WHERE route = ? AND handler = ? AND principal = ? AND idempotency_key = ? AND reservation_token = ?
            """;

    /**
     * Used only by {@link #await} to block on a row without becoming its
     * owner: a plain {@code SELECT ... FOR UPDATE} can't do this - an
     * in-flight, uncommitted {@code INSERT} is invisible to every other
     * transaction under MVCC, so a bare {@code SELECT} would see nothing and
     * return instantly rather than wait. Conflict detection on {@code INSERT
     * ... ON CONFLICT} is what actually blocks (the same primitive {@link
     * #UPSERT_RESERVATION} uses). {@code DO NOTHING} rather than {@code DO
     * UPDATE}: {@code await()} rolls back immediately if this "wins" (see
     * below), so what the conflict branch would have done never matters.
     */
    private static final String TRY_LOCK = """
            INSERT INTO idempotency_record
                (route, handler, principal, idempotency_key, fingerprint, reservation_token, created_at, expires_at)
            VALUES (?, ?, ?, ?, '__await__', '__await__', clock_timestamp(), clock_timestamp())
            ON CONFLICT (route, handler, principal, idempotency_key) DO NOTHING
            RETURNING route
            """;

    /**
     * Bounds the reservation's row-lock wait for the current transaction
     * only (the {@code SET LOCAL} equivalent, but bindable), returning the
     * value in effect before - {@link #reserve} restores it once the
     * reservation is won, since the handler then runs its own statements in
     * this same transaction and must not inherit the store's timeout. The
     * {@code OFFSET 0} keeps the subquery from being flattened, so the old
     * value is read before {@code set_config} replaces it.
     */
    private static final String SWAP_LOCK_TIMEOUT = """
            SELECT previous.value, set_config('lock_timeout', ?, true)
            FROM (SELECT current_setting('lock_timeout') AS value OFFSET 0) previous
            """;

    private static final String SET_LOCK_TIMEOUT = "SELECT set_config('lock_timeout', ?, true)";

    /** SQLState 55P03 ("lock_not_available") - raised when {@code lock_timeout} is exceeded. */
    private static final String SQLSTATE_LOCK_NOT_AVAILABLE = "55P03";

    /**
     * Bounded retries for the boundary race where the reservation upsert loses
     * the conflict (the row wasn't stale enough to supersede) yet the row is
     * already expired to read - {@code clock_timestamp()} advanced between the
     * upsert's {@code expires_at < clock_timestamp()} check and {@code
     * SELECT_RECORD}'s {@code expires_at > clock_timestamp()} filter. A retry's
     * later {@code clock_timestamp()} can now supersede the stale row. If it
     * keeps racing past this bound, {@code reserve} surfaces {@link
     * StoreUnavailableException} so the {@code onStoreFailure} posture applies
     * rather than leaking a raw 500.
     */
    private static final int MAX_RESERVE_ATTEMPTS = 8;

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
        abandonLeakedScope(key);
        TransactionStatus status;
        try {
            status = transactionManager.getTransaction(new DefaultTransactionDefinition());
        } catch (RuntimeException e) {
            throw asStoreUnavailableIfConnectionFailure(e);
        }
        String fenceToken = UUID.randomUUID().toString();
        try {
            String callerLockTimeout = jdbc.queryForObject(SWAP_LOCK_TIMEOUT, (rs, rowNum) -> rs.getString(1), lockTimeout.toMillis() + "ms");
            for (int attempt = 0; attempt < MAX_RESERVE_ATTEMPTS; attempt++) {
                Object[] args = concat(keyArgs(key), new Object[] {fingerprint, fenceToken, toPgInterval(lockTtl)});
                boolean won = !jdbc.query(UPSERT_RESERVATION, (rs, rowNum) -> rs.getString(1), args).isEmpty();
                if (won) {
                    // The handler runs in this same transaction from here on -
                    // hand it back the lock_timeout it would have had.
                    jdbc.queryForObject(SET_LOCK_TIMEOUT, String.class, callerLockTimeout);
                    bindScope(status, key, fenceToken);
                    return ReservationResult.reserved(fenceToken);
                }

                // Lost the conflict: either a genuinely different, still-valid
                // record exists, or (nested call on a thread with an already-open
                // scope) it's our own not-yet-superseded reservation.
                Optional<IdempotencyRecord> existing = findRecord(key);
                if (existing.isPresent()) {
                    if (!status.isNewTransaction()) {
                        // Joined a transaction the caller keeps using after this returns.
                        jdbc.queryForObject(SET_LOCK_TIMEOUT, String.class, callerLockTimeout);
                    }
                    transactionManager.commit(status);
                    return ReservationResult.exists(existing.get());
                }
                // Boundary race: lost the conflict, yet the row is already
                // expired to read. clock_timestamp() advances on the retry, so
                // the stale row can now be superseded rather than throwing.
            }
            // Still racing past the retry bound - surface as unavailable so the
            // posture applies (rolled back by the catch below), never a raw 500.
            throw new StoreUnavailableException(
                    "reservation for " + key + " kept racing (lost the conflict but the record vanished on every retry)");
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

    /**
     * {@code false}: the reservation is the reserving thread's open, thread-bound transaction,
     * so an asynchronous handler that finishes on another thread could never commit or roll it
     * back - and would leave it bound to a pooled servlet thread (issue 064).
     */
    @Override
    public boolean supportsAsyncHandlers() {
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

    /**
     * Overrides the polling default with a real blocking wait (ADR 0001/0002):
     * {@link #TRY_LOCK} blocks on the same conflict the primary's open
     * transaction holds, and unblocks the instant it commits or rolls back -
     * not on the next {@code pollInterval} tick. {@code pollInterval}/{@code
     * pollJitter} are unused here; native blocking replaces the poll cadence
     * entirely.
     *
     * <p>Deliberately never keeps ownership, matching {@code
     * IdempotencyEngine}'s WAIT-mode contract (no self-promotion): if {@code
     * TRY_LOCK} "wins" - the row was gone because the primary rolled back -
     * that win is immediately rolled back too, and this reports {@code
     * Optional.empty()} rather than racing to claim it.
     */
    @Override
    public Optional<IdempotencyRecord> await(EffectiveKey key, Duration waitTimeout, Duration pollInterval, Duration pollJitter) {
        // Monotonic, like IdempotencyStore's default: a stepping wall clock
        // must not cut the wait short or stretch it.
        long deadline = System.nanoTime() + waitTimeout.toNanos();
        while (true) {
            Duration remaining = Duration.ofNanos(deadline - System.nanoTime());
            if (remaining.isZero() || remaining.isNegative()) {
                return findRecord(key);
            }
            TransactionStatus status;
            try {
                status = transactionManager.getTransaction(new DefaultTransactionDefinition());
            } catch (RuntimeException e) {
                throw asStoreUnavailableIfConnectionFailure(e);
            }
            try {
                jdbc.execute("SET LOCAL lock_timeout = '" + remaining.toMillis() + "ms'");
                boolean wouldHaveWon = !jdbc.query(TRY_LOCK, (rs, rowNum) -> rs.getString(1), keyArgs(key)).isEmpty();
                if (wouldHaveWon) {
                    // The row was gone (primary released/crashed) - discard
                    // our own accidental claim rather than keep it.
                    transactionManager.rollback(status);
                    return Optional.empty();
                }
                Optional<IdempotencyRecord> current = findRecord(key);
                transactionManager.commit(status);
                if (current.isEmpty() || current.get().isCompleted()) {
                    return current;
                }
                // Unblocked but still in-progress (a new primary claimed the
                // row the instant ours let go) - loop and wait on it too.
            } catch (RuntimeException e) {
                safeRollback(status);
                if (isLockTimeout(e)) {
                    // waitTimeout exhausted while genuinely still blocked - the
                    // primary's reservation is still open and uncommitted, so
                    // (like reserve()'s own lock-timeout branch above) a plain
                    // SELECT can't see it: an in-flight INSERT is invisible to
                    // every other transaction under MVCC, so findRecord(key)
                    // here would wrongly report empty ("released") instead of
                    // still-in-progress ("timeout"). No fingerprint is
                    // available to carry (unlike reserve()'s caller-supplied
                    // one) - none is needed here, since the engine's WAIT-mode
                    // timeout branch only inspects IdempotencyRecord#isCompleted.
                    return Optional.of(new IdempotencyRecord(RecordState.IN_PROGRESS, null, null));
                }
                throw asStoreUnavailableIfConnectionFailure(e);
            }
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
            // Whatever the cause (connection lost, a constraint deferred to
            // commit, the transaction marked rollback-only by the handler),
            // the handler's writes shared this transaction and are gone with
            // it - unlike a best-effort store, this is not "only the caching
            // was lost", so it must not be reported as StoreUnavailableException.
            safeRollback(current.status());
            log.warn("Postgres idempotency completion failed - the effect was rolled back: {}", key, e);
            throw new EffectRolledBackException("the transaction for " + key + " could not be committed", e);
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
    private void bindScope(TransactionStatus status, EffectiveKey key, String fenceToken) {
        ReservationScope existing = scope.get();
        scope.set(existing == null ? new ReservationScope(status, key, fenceToken) : new ReservationScope(existing.status(), key, fenceToken));
    }

    /**
     * Rolls back a reservation for a <em>different</em> key still bound to this
     * thread when a new {@code reserve()} begins (issue 033). In the supported
     * synchronous model {@code complete()}/{@code release()} always resolve a
     * reservation before its thread serves anything else, so a leftover scope
     * for another key means one was never resolved - an async or thread-hopping
     * handler, or a path where the interceptor never ran. Joining it would let
     * this reservation's {@code complete()} commit the leaked one as a side
     * effect, leaving a committed {@code IN_PROGRESS} row that blocks genuine
     * retries until it expires. Rolling it back instead frees its key, returns
     * its connection to the pool, and starts this reservation in a fresh
     * transaction.
     *
     * <p>A leftover scope for the <em>same</em> key is left alone: that is the
     * nested same-thread re-reservation the shared contract suite exercises
     * (a repeat reservation, or superseding one's own expired reservation).
     */
    private void abandonLeakedScope(EffectiveKey key) {
        ReservationScope leaked = scope.get();
        if (leaked == null || leaked.key().equals(key)) {
            return;
        }
        log.warn("Rolling back an unresolved Postgres idempotency reservation left on this thread for {} "
                + "(complete/release never ran - is an async handler or a custom caller using this store?)", leaked.key());
        scope.remove();
        safeRollback(leaked.status());
    }

    private Optional<IdempotencyRecord> findRecord(EffectiveKey key) {
        return jdbc.query(SELECT_RECORD, this::mapRecord, keyArgs(key)).stream().findFirst();
    }

    /** Column sizes of the effective-key columns in {@code V1__idempotency_record.sql}. */
    private static final int ROUTE_COLUMN_LENGTH = 512;
    private static final int HANDLER_COLUMN_LENGTH = 255;
    private static final int PRINCIPAL_COLUMN_LENGTH = 255;
    private static final int KEY_COLUMN_LENGTH = 255;

    /** Marks a stored component as a digest of the real, over-long value. */
    private static final String DIGEST_PREFIX = "sha256:";

    private static Object[] keyArgs(EffectiveKey key) {
        return new Object[] {
                fitColumn(key.route(), ROUTE_COLUMN_LENGTH),
                fitColumn(key.handler(), HANDLER_COLUMN_LENGTH),
                fitColumn(key.principal(), PRINCIPAL_COLUMN_LENGTH),
                fitColumn(key.value(), KEY_COLUMN_LENGTH)};
    }

    /**
     * Stores a key component verbatim when it fits its column, and otherwise as
     * {@code sha256:<hex>} of its UTF-8 bytes (71 characters, which fits every
     * key column) - issue 061. The route (request URI), principal (a token
     * claim) and listener id are unbounded, and {@code idempotency.key.max-length}
     * may exceed 255; inserting such a value verbatim fails with SQLState 22001,
     * a raw 500 that bypasses {@code onStoreFailure}. The mapping is
     * deterministic, so reserve/find/complete/await all address the same row,
     * and a value that fits is stored exactly as before, so existing rows are
     * unaffected and no migration is needed. Lengths are counted in code
     * points, the unit {@code VARCHAR(n)} limits. A verbatim value can't
     * collide with a digest without a SHA-256 preimage.
     */
    static String fitColumn(String value, int columnLength) {
        if (value.codePointCount(0, value.length()) <= columnLength) {
            return value;
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return DIGEST_PREFIX + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
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

    private record ReservationScope(TransactionStatus status, EffectiveKey key, String fenceToken) {
    }
}
