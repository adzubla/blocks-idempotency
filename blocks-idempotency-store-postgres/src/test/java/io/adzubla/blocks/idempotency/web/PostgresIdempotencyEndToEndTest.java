package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.postgres.PostgresIdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
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
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end MockMvc coverage of {@code @Idempotent(store = PostgresIdempotencyStore.QUALIFIER)} over
 * a real Testcontainers Postgres: the interceptor + engine + {@link
 * PostgresIdempotencyStore}, driven over HTTP,
 * proving Slice 017's acceptance criteria and the ADR 0003 transaction-join
 * design it implements.
 */
@Testcontainers
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = PostgresIdempotencyEndToEndTest.TestApplication.class,
        properties = "idempotency.max-body-size=32B")
@AutoConfigureMockMvc
class PostgresIdempotencyEndToEndTest {

    @Container
    @ServiceConnection
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AtomicInteger handlerInvocations;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void resetFixtures() {
        handlerInvocations.set(0);
        jdbc.update("DELETE FROM test_orders");
        jdbc.update("DELETE FROM idempotency_record");
    }

    @Test
    void firstRequestExecutesTheHandlerAndCachesThe2xxResponse() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").exists());

        assertThat(handlerInvocations.get()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
    }

    @Test
    void repeatWithSameKeyReplaysWithoutReExecutingTheHandler() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"));

        assertThat(handlerInvocations.get()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
    }

    @Test
    void sameKeyWithADifferentBodyIsRejectedWith422() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-3")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-3")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":20}"))
                .andExpect(status().isUnprocessableContent())
                .andExpect(header().string("Idempotency-Reject-Reason", "collision"));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void aNonSuccessResponseRollsBackTheReservationAndTheHandlersOwnWriteTogether() throws Exception {
        mockMvc.perform(post("/flaky")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-flaky-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"mode\":\"error\"}"))
                .andExpect(status().isInternalServerError());

        assertThat(handlerInvocations.get()).isEqualTo(1);
        // The handler's own INSERT ran in the same transaction as the
        // reservation - releasing (rolling back) the reservation on a
        // non-2xx undoes the business write too, proving the two share a
        // transaction rather than the reservation being a side channel.
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isZero();

        mockMvc.perform(post("/flaky")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-flaky-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"mode\":\"ok\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"));

        assertThat(handlerInvocations.get()).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
    }

    @Test
    void aThrownExceptionRollsBackTheReservationAndTheHandlersOwnWriteTogether() throws Exception {
        assertThatThrownBy(() -> mockMvc.perform(post("/flaky")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-flaky-2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"mode\":\"throw\"}")))
                .hasRootCauseInstanceOf(IllegalStateException.class);

        assertThat(handlerInvocations.get()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isZero();
    }

    @Test
    void aHandlerWithNoTransactionalAnnotationStillSharesTheReservationsTransaction() throws Exception {
        // No @Transactional on this endpoint at all - Spring's thread-bound
        // resource binding means the plain JdbcTemplate write still joins the
        // already-open reservation transaction (ADR 0003), and a non-2xx
        // still rolls both back together.
        mockMvc.perform(post("/orders-no-transactional")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-4")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10,\"mode\":\"error\"}"))
                .andExpect(status().isInternalServerError());

        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isZero();

        mockMvc.perform(post("/orders-no-transactional")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-4")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10,\"mode\":\"ok\"}"))
                .andExpect(status().isCreated());

        assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
    }

    @Test
    void aResponseOverMaxBodySizeIsCompletedButNotReplayable() throws Exception {
        mockMvc.perform(post("/orders-huge")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-5")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated());

        assertThat(handlerInvocations.get()).isEqualTo(1);

        mockMvc.perform(post("/orders-huge")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-5")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isConflict())
                .andExpect(header().doesNotExist("Retry-After"))
                .andExpect(header().string("Idempotency-Reject-Reason", "response_unavailable"));

        // Terminal - no re-execution.
        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    /**
     * Two genuinely concurrent requests (real threads via MockMvc) with the
     * same key, proving Slice 018's native-concurrency outcomes end-to-end
     * through the real interceptor/engine/store stack, not just the store in
     * isolation ({@code PostgresNativeConcurrencyTest}).
     */
    @Test
    void twoConcurrentRequestsWithTheSameKeyExecuteTheHandlerExactlyOnceWhenThePrimarySucceeds() throws Exception {
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<MvcResult> first = executor.submit(() -> concurrentOrderRequest("ok").andReturn());
            Thread.sleep(100); // let the first request win the reservation and enter its sleep
            Future<MvcResult> second = executor.submit(() -> concurrentOrderRequest("ok").andReturn());

            MvcResult r1 = first.get(5, TimeUnit.SECONDS);
            MvcResult r2 = second.get(5, TimeUnit.SECONDS);

            assertThat(handlerInvocations.get()).isEqualTo(1);
            assertThat(r1.getResponse().getStatus()).isEqualTo(201);
            assertThat(r2.getResponse().getStatus()).isEqualTo(201);
            long replays = Stream.of(r1, r2).filter(r -> "true".equals(r.getResponse().getHeader("Idempotency-Replayed"))).count();
            assertThat(replays).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
        }
    }

    @Test
    void whenThePrimaryThrowsTheBlockedConcurrentRequestSelfPromotesAndExecutesTheEffectExactlyOnce() throws Exception {
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<MvcResult> primary = executor.submit(() -> concurrentOrderRequest("throw").andReturn());
            Thread.sleep(100); // let the primary win the reservation and enter its sleep
            Future<MvcResult> waiter = executor.submit(() -> concurrentOrderRequest("ok").andReturn());

            assertThatThrownBy(() -> primary.get(5, TimeUnit.SECONDS)).hasRootCauseInstanceOf(IllegalStateException.class);
            MvcResult waiterResult = waiter.get(5, TimeUnit.SECONDS);

            // handlerInvocations counts every attempt regardless of outcome
            // (primary's + the self-promoted waiter's) - the effect itself
            // (the row) is what must land exactly once, not zero or twice.
            assertThat(handlerInvocations.get()).isEqualTo(2);
            assertThat(waiterResult.getResponse().getStatus()).isEqualTo(201);
            assertThat(waiterResult.getResponse().getHeader("Idempotency-Replayed")).isNull();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM test_orders", Integer.class)).isEqualTo(1);
        }
    }

    private ResultActions concurrentOrderRequest(String mode) throws Exception {
        return mockMvc.perform(post("/orders-concurrent")
                .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "concurrent-key")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"mode\":\"" + mode + "\"}"));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean
        AtomicInteger handlerInvocations() {
            return new AtomicInteger();
        }

        @Bean
        OrdersController ordersController(JdbcTemplate jdbc, AtomicInteger handlerInvocations) {
            return new OrdersController(jdbc, handlerInvocations);
        }
    }

    @RestController
    static class OrdersController {

        private final JdbcTemplate jdbc;
        private final AtomicInteger handlerInvocations;

        OrdersController(JdbcTemplate jdbc, AtomicInteger handlerInvocations) {
            this.jdbc = jdbc;
            this.handlerInvocations = handlerInvocations;
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @PostMapping("/orders")
        @Transactional
        ResponseEntity<Map<String, Object>> createOrder(@RequestBody Map<String, Object> body) {
            handlerInvocations.incrementAndGet();
            Integer orderId = jdbc.queryForObject(
                    "INSERT INTO test_orders(amount) VALUES (?) RETURNING id", Integer.class, body.get("amount"));
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @PostMapping("/orders-no-transactional")
        ResponseEntity<Map<String, Object>> createOrderNoTransactional(@RequestBody Map<String, Object> body) {
            handlerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", body.get("amount"));
            if ("error".equals(body.get("mode"))) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", "boom"));
            }
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("ok", true));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @PostMapping("/flaky")
        @Transactional
        ResponseEntity<Map<String, Object>> createFlaky(@RequestBody Map<String, Object> body) {
            handlerInvocations.incrementAndGet();
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 999);
            String mode = (String) body.get("mode");
            if ("error".equals(mode)) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", "boom"));
            }
            if ("throw".equals(mode)) {
                throw new IllegalStateException("boom");
            }
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("ok", true));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @PostMapping("/orders-huge")
        ResponseEntity<String> createHugeOrder() {
            handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body("response body over the 32-byte configured max-body-size");
        }

        /**
         * Sleeps mid-handler so a genuinely concurrent second request has a
         * wide window to observe {@code reserve()} blocking on the reservation
         * row's lock (Slice 018), rather than the race being too tight to
         * reliably exercise in a test.
         */
        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @PostMapping("/orders-concurrent")
        @Transactional
        ResponseEntity<Map<String, Object>> createOrderSlowly(@RequestBody Map<String, Object> body) throws InterruptedException {
            handlerInvocations.incrementAndGet();
            Thread.sleep(300);
            jdbc.update("INSERT INTO test_orders(amount) VALUES (?)", 1);
            if ("throw".equals(body.get("mode"))) {
                throw new IllegalStateException("boom");
            }
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("ok", true));
        }
    }
}
