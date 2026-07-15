package io.adzubla.blocks.idempotency.web;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.StoreUnavailableException;
import io.adzubla.blocks.idempotency.store.redis.RedisIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end proof that a genuine Redis outage (the container stopped, not
 * simulated) drives the existing {@code onStoreFailure} posture correctly
 * through the real interceptor/engine/{@link
 * RedisIdempotencyStore} stack (Slice 015). The
 * posture logic itself is already unit-tested generically in {@code
 * idempotency-core} (Slice 007, {@code InMemoryIdempotencyStore.setUnavailable});
 * this instead proves the wiring: that a real store failure actually reaches
 * that logic as a {@link StoreUnavailableException}.
 *
 * <p>A dedicated container, stopped mid-class and never restarted - this test
 * class's Redis is a one-way trip, same reasoning as {@code
 * RedisIdempotencyStoreOutageTest}.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, classes = RedisIdempotencyOutageEndToEndTest.TestApplication.class)
@AutoConfigureMockMvc
class RedisIdempotencyOutageEndToEndTest {

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getRedisHost);
        registry.add("spring.data.redis.port", REDIS::getRedisPort);
        // Short command timeout: Lettuce's default (60s) would make every
        // request against the stopped container in this test take a minute.
        registry.add("spring.data.redis.timeout", () -> "2s");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AtomicInteger handlerInvocations;

    @Test
    void aStoppedRedisFailsOpenByDefaultAndFailsClosedWhenConfiguredTo() throws Exception {
        // Sanity check while Redis is still up: both endpoints work normally.
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

        REDIS.stop();

        // Default posture (fail-open, ADR/CONTEXT.md "Store failure"): the
        // handler still runs, unprotected, rather than taking the endpoint
        // down over a Redis blip.
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
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Idempotency-Reject-Reason", "store_unavailable"));
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

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = RedisIdempotencyStore.QUALIFIER)
        @PostMapping("/orders-open")
        ResponseEntity<Map<String, Object>> createOrderFailOpen() {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = RedisIdempotencyStore.QUALIFIER, onStoreFailure = Idempotent.OnStoreFailure.CLOSED)
        @PostMapping("/orders-closed")
        ResponseEntity<Map<String, Object>> createOrderFailClosed(@RequestBody(required = false) Map<String, Object> body) {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }
    }
}
