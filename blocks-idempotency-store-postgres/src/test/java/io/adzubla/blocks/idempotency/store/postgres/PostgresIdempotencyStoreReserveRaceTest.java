package io.adzubla.blocks.idempotency.store.postgres;

import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import io.adzubla.blocks.idempotency.store.StoreUnavailableException;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The Postgres side of the reservation re-read race
 * (docs/issues/029-store-reserve-conflict-vanished-record-500.md), the same
 * defect fixed symmetrically with {@code RedisIdempotencyStore}. The upsert can
 * lose the conflict (the row wasn't stale enough to supersede) while {@code
 * SELECT_RECORD} then sees nothing (the row is already expired to read) -
 * {@code clock_timestamp()} advanced between the two statements. Before the fix
 * that threw a bare {@code IllegalStateException} -> raw 500, bypassing the
 * {@code onStoreFailure} posture. Now {@code reserve()} retries (a later {@code
 * clock_timestamp()} supersedes the stale row) and, only if it keeps racing,
 * surfaces {@link StoreUnavailableException}.
 *
 * <p>Pure Mockito, no Postgres: the upsert and select queries are distinguished
 * by their SQL text.
 */
class PostgresIdempotencyStoreReserveRaceTest {

    private final EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-1");

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void reserveRetriesTheBoundaryRaceAndWinsOnTheRetry() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        when(transactionManager.getTransaction(any())).thenReturn(status);
        // Upsert: lost the conflict first, wins on the retry.
        when(jdbc.query(contains("INSERT INTO"), any(RowMapper.class), any(Object[].class)))
                .thenReturn(List.of(), List.of("POST"));
        // Select in between: the row is already expired to read (vanished).
        when(jdbc.query(contains("SELECT"), any(RowMapper.class), any(Object[].class)))
                .thenReturn(List.of());

        PostgresIdempotencyStore store =
                new PostgresIdempotencyStore(jdbc, transactionManager, new ObjectMapper(), Duration.ofSeconds(2));
        try {
            ReservationResult result = store.reserve(key, "fp", Duration.ofSeconds(30));
            assertThat(result.outcome()).isEqualTo(ReservationResult.Outcome.RESERVED);
            assertThat(result.fenceToken()).isPresent();
        } finally {
            store.abandonDanglingReservationForTests();
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void reserveThatKeepsRacingSurfacesAsStoreUnavailableAndRollsBack() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        TransactionStatus status = mock(TransactionStatus.class);
        when(transactionManager.getTransaction(any())).thenReturn(status);
        // Every upsert loses; every select finds the row already gone.
        when(jdbc.query(contains("INSERT INTO"), any(RowMapper.class), any(Object[].class)))
                .thenReturn(List.of());
        when(jdbc.query(contains("SELECT"), any(RowMapper.class), any(Object[].class)))
                .thenReturn(List.of());

        PostgresIdempotencyStore store =
                new PostgresIdempotencyStore(jdbc, transactionManager, new ObjectMapper(), Duration.ofSeconds(2));

        assertThatThrownBy(() -> store.reserve(key, "fp", Duration.ofSeconds(30)))
                .isInstanceOf(StoreUnavailableException.class);
        // The open transaction must be rolled back, not leaked.
        verify(transactionManager).rollback(status);
    }
}
