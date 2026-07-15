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
 * interval (Slice 018) - Postgres has no native TTL (unlike Redis), so
 * completed or abandoned reservations would otherwise accumulate in the table
 * forever.
 *
 * <p>Runs its own single-thread {@link ScheduledExecutorService} rather than
 * riding Spring's {@code @Scheduled}/{@code TaskScheduler} machinery: the
 * interval is an already-bound {@link Duration} ({@link PostgresStoreProperties}),
 * so there's no property string to re-parse, and this works in any context
 * regardless of whether task-scheduling auto-configuration happens to be
 * present.
 */
public class PostgresIdempotencyCleanupJob implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(PostgresIdempotencyCleanupJob.class);

    private static final String DELETE_EXPIRED = "DELETE FROM idempotency_record WHERE expires_at < clock_timestamp()";

    private final JdbcTemplate jdbc;
    private final Duration interval;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(PostgresIdempotencyCleanupJob::newDaemonThread);

    public PostgresIdempotencyCleanupJob(JdbcTemplate jdbc, Duration interval) {
        this.jdbc = jdbc;
        this.interval = interval;
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

    /** Deletes every row past its {@code expires_at}; returns the number of rows deleted. Public so a test can trigger it directly, without waiting on the schedule. */
    public int deleteExpiredRecords() {
        return jdbc.update(DELETE_EXPIRED);
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
