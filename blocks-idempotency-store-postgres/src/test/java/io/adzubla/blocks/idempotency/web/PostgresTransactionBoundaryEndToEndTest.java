package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.postgres.PostgresIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The edges of the transaction {@link PostgresIdempotencyStore} shares with
 * the handler (ADR 0003): a commit that fails after the handler has already
 * produced a 2xx must not reach the client as a success, and the store's own
 * {@code lock_timeout} must not leak into the handler's statements.
 */
@Testcontainers
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = PostgresTransactionBoundaryEndToEndTest.TestApplication.class,
        properties = "idempotency.default-store=" + PostgresIdempotencyStore.QUALIFIER)
@AutoConfigureMockMvc
class PostgresTransactionBoundaryEndToEndTest {

    @Container
    @ServiceConnection
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void aFailedCommitIsReportedAsAnErrorNotAsTheHandlersSuccess() throws Exception {
        MvcResult result = mockMvc.perform(post("/orders-rollback-only").header(Idempotent.IDEMPOTENCY_KEY_HEADER, "commit-fail-1")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"amount\":10}"))
                .andExpect(status().isInternalServerError())
                .andReturn();

        assertThat(result.getResponse().getContentAsString()).isEmpty();
        assertThat(result.getResponse().getHeader("Location")).isNull();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM idempotency_record WHERE idempotency_key = 'commit-fail-1'", Integer.class))
                .isZero();
    }

    @Test
    void theStoresLockTimeoutDoesNotLeakIntoTheHandlersTransaction() throws Exception {
        String connectionDefault = jdbc.queryForObject("SHOW lock_timeout", String.class);

        mockMvc.perform(post("/lock-timeout").header(Idempotent.IDEMPOTENCY_KEY_HEADER, "lock-timeout-1")
                        .contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lockTimeout").value(connectionDefault));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean
        BoundaryController boundaryController(JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
            return new BoundaryController(jdbc, transactionManager);
        }
    }

    @RestController
    static class BoundaryController {

        private final JdbcTemplate jdbc;
        private final PlatformTransactionManager transactionManager;

        BoundaryController(JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
            this.jdbc = jdbc;
            this.transactionManager = transactionManager;
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @PostMapping("/orders-rollback-only")
        ResponseEntity<Map<String, Object>> createOrderThenMarkRollbackOnly(@RequestBody Map<String, Object> body) {
            jdbc.update("INSERT INTO test_orders (amount) VALUES (10)");
            // What an inner @Transactional(REQUIRED) call that threw - and was caught
            // here - leaves behind: the shared transaction marked rollback-only.
            TransactionStatus inner = transactionManager.getTransaction(new DefaultTransactionDefinition());
            transactionManager.rollback(inner);
            return ResponseEntity.status(HttpStatus.CREATED).header("Location", "/orders/1").body(Map.of("created", true));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @PostMapping("/lock-timeout")
        ResponseEntity<Map<String, Object>> lockTimeout(@RequestBody Map<String, Object> body) {
            return ResponseEntity.ok(Map.of("lockTimeout", jdbc.queryForObject("SHOW lock_timeout", String.class)));
        }
    }
}
