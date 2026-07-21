package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Proves Slice 007's "the global default is respected" acceptance criterion
 * end-to-end: with {@code idempotency.default-on-store-failure=CLOSED} and no
 * per-endpoint {@code onStoreFailure} override, a store outage yields 503.
 * Deliberately a separate Spring context (and test class) from {@link
 * IdempotencyEndToEndTest}, so this global override doesn't disturb that
 * class's fail-open assumptions for its other tests.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = StoreFailureGlobalDefaultEndToEndTest.TestApplication.class)
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "idempotency.default-on-store-failure=CLOSED",
        "idempotency.default-store=" + InMemoryIdempotencyStore.QUALIFIER
})
class StoreFailureGlobalDefaultEndToEndTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AtomicInteger handlerInvocations;

    @Autowired
    private IdempotencyStore idempotencyStore;

    @Test
    void storeUnavailableFallsBackToTheGlobalFailClosedDefault() throws Exception {
        ((InMemoryIdempotencyStore) idempotencyStore).setUnavailable(true);

        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isServiceUnavailable());

        assertThat(handlerInvocations.get()).isZero();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean
        AtomicInteger handlerInvocations() {
            return new AtomicInteger();
        }

        @Bean(InMemoryIdempotencyStore.QUALIFIER)
        IdempotencyStore idempotencyStore() {
            return new InMemoryIdempotencyStore();
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

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @PostMapping("/orders")
        ResponseEntity<Map<String, Object>> createOrder(@RequestBody Map<String, Object> body) {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }
    }
}
