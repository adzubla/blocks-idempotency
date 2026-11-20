package io.adzubla.blocks.idempotency.store.postgres;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Deletes {@code idempotency_record} rows past {@code expires_at} on a fixed
 * delay - the configured interval elapses between the end of one sweep and the
 * start of the next (Slice 018) - Postgres has no native TTL (unlike Redis), so
 * completed or abandoned reservations would otherwise accumulate in the table
 * forever.
 *
 * <p>Runs its own single-thread {@link ScheduledExecutorService} rather than
 * riding Spring's {@code @Scheduled}/{@code TaskScheduler} machinery: the
 * interval is an already-bound {@link Duration} ({@link PostgresStoreProperties}),
 * so there's no property string to re-parse, and this works in any context
 * regardless of whether task-scheduling auto-configuration happens to be
 * present.
 *
 * <p>Each sweep deletes in bounded batches, each its own short auto-committed
 * statement, rather than one {@code DELETE} of everything expired. That avoids
 * long-running deletes and WAL spikes at volume (issue 066). {@code FOR UPDATE SKIP
 * LOCKED} makes a batch skip rows another transaction holds - an expired row an open
 * reservation is currently superseding, or rows a concurrent sweep from another
 * instance already claimed - instead of waiting on them. A skipped row is deleted
 * by the sweep holding it, live again once an open reservation commits, or picked
 * up by a later sweep.
 */
public class PostgresIdempotencyCleanupJob implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(PostgresIdempotencyCleanupJob.class);

    /** Rows deleted per batch when no batch size is configured. */
    public static final int DEFAULT_BATCH_SIZE = 1000;

    private static final String DELETE_EXPIRED_BATCH = """
            DELETE FROM idempotency_record
            WHERE ctid IN (
                SELECT ctid FROM idempotency_record
                WHERE expires_at < clock_timestamp()
                LIMIT ?
                FOR UPDATE SKIP LOCKED)
            """;

    private final JdbcTemplate jdbc;
    private final Duration interval;
    private final int batchSize;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(PostgresIdempotencyCleanupJob::newDaemonThread);

    public PostgresIdempotencyCleanupJob(JdbcTemplate jdbc, Duration interval) {
        this(jdbc, interval, DEFAULT_BATCH_SIZE);
    }

    public PostgresIdempotencyCleanupJob(JdbcTemplate jdbc, Duration interval, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("idempotency.postgres.cleanup.batch-size must be at least 1, was " + batchSize);
        }
        this.jdbc = jdbc;
        this.interval = interval;
        this.batchSize = batchSize;
    }

    @Override
    public void afterPropertiesSet() {
        long periodMillis = interval.toMillis();
        executor.scheduleWithFixedDelay(this::runSweep, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    /** Wraps {@link #deleteExpiredRecords()} so a failed sweep is logged instead of silently killing the schedule. */
    private void runSweep() {
        try {
            int deleted = deleteExpiredRecords();
            log.debug("Postgres idempotency cleanup swept {} expired record(s)", deleted);
        } catch (RuntimeException e) {
            // ScheduledExecutorService silently drops all future runs if a task
            // throws - log it rather than let the sweep die without a trace.
            log.error("Postgres idempotency cleanup sweep failed", e);
        }
    }

    /**
     * Deletes every row past its {@code expires_at} that no other transaction holds, in batches of
     * {@code batchSize}; returns the number of rows deleted. Public so a test can trigger it
     * directly, without waiting on the schedule.
     */
    public int deleteExpiredRecords() {
        int total = 0;
        int deleted;
        do {
            deleted = jdbc.update(DELETE_EXPIRED_BATCH, batchSize);
            total += deleted;
        } while (deleted == batchSize);
        return total;
    }

    @Override
    public void destroy() {
        executor.shutdownNow();
    }

    private static Thread newDaemonThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "idempotency-postgres-cleanup");
        thread.setDaemon(true);
        return thread;
    }
}
