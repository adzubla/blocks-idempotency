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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end proof that a genuine Postgres outage (the container stopped, not
 * simulated) drives the existing {@code onStoreFailure} posture correctly
 * through the real interceptor/engine/{@link
 * PostgresIdempotencyStore} stack - the
 * Postgres counterpart of {@code RedisIdempotencyOutageEndToEndTest}, closing
 * the gap where {@code PostgresIdempotencyStore} previously never translated a
 * connection failure into {@code StoreUnavailableException} at all (the PRD's
 * "store-failure posture uniformly ... across both stores").
 *
 * <p>A dedicated container, stopped mid-class and never restarted - same
 * one-way-trip reasoning as {@link io.adzubla.blocks.idempotency.store.postgres.PostgresIdempotencyStoreOutageTest}.
 */
@Testcontainers
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = PostgresIdempotencyOutageEndToEndTest.TestApplication.class,
        // Short connection timeout - Hikari's default (30s) would make every
        // request against the stopped container in this test take half a minute.
        properties = "spring.datasource.hikari.connection-timeout=2000")
@AutoConfigureMockMvc
class PostgresIdempotencyOutageEndToEndTest {

    @Container
    @ServiceConnection
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AtomicInteger handlerInvocations;

    @Test
    void aStoppedPostgresFailsOpenByDefaultAndFailsClosedWhenConfiguredTo() throws Exception {
        // Sanity check while Postgres is still up: both endpoints work normally.
        mockMvc.perform(post("/orders-open")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "sanity-open")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated());
        mockMvc.perform(post("/orders-closed")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "sanity-closed")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated());
        int invocationsBeforeOutage = handlerInvocations.get();

        POSTGRES.stop();

        // Default posture (fail-open): the handler still runs, unprotected,
        // rather than taking the endpoint down over a database blip.
        mockMvc.perform(post("/orders-open")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "outage-open")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated());
        assertThat(handlerInvocations.get()).isEqualTo(invocationsBeforeOutage + 1);

        // onStoreFailure=CLOSED: a critical endpoint refuses to run
        // unprotected - 503, handler never invoked.
        mockMvc.perform(post("/orders-closed")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "outage-closed")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isServiceUnavailable());
        assertThat(handlerInvocations.get()).isEqualTo(invocationsBeforeOutage + 1);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean
        AtomicInteger handlerInvocations() {
            return new AtomicInteger();
        }

        @Bean
        OrdersController ordersController(AtomicInteger handlerInvocations) {
            return new OrdersController(handlerInvocations);
        }
    }

    @RestController
    static class OrdersController {

        private final AtomicInteger handlerInvocations;

        OrdersController(AtomicInteger handlerInvocations) {
            this.handlerInvocations = handlerInvocations;
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @PostMapping("/orders-open")
        ResponseEntity<Map<String, Object>> createOrderFailOpen() {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = PostgresIdempotencyStore.QUALIFIER, onStoreFailure = Idempotent.OnStoreFailure.CLOSED)
        @PostMapping("/orders-closed")
        ResponseEntity<Map<String, Object>> createOrderFailClosed(@RequestBody(required = false) Map<String, Object> body) {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }
    }
}
