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
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * A response over {@code max-body-size} on a store that doesn't commit the effect
 * is streamed to the client rather than buffered in full, reaches the client
 * intact, and is completed without a body, so a duplicate gets {@code
 * response_unavailable}.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = OversizedResponseEndToEndTest.TestApplication.class,
        properties = {"idempotency.default-store=" + InMemoryIdempotencyStore.QUALIFIER, "idempotency.max-body-size=32B"})
@AutoConfigureMockMvc
class OversizedResponseEndToEndTest {

    private static final String LARGE_BODY = "x".repeat(10_000);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AtomicInteger handlerInvocations;

    @Test
    void anOversizedResponseReachesTheClientIntactAndIsNotReplayable() throws Exception {
        mockMvc.perform(post("/large").header(Idempotent.IDEMPOTENCY_KEY_HEADER, "large-1"))
                .andExpect(status().isCreated())
                .andExpect(content().string(LARGE_BODY));

        mockMvc.perform(post("/large").header(Idempotent.IDEMPOTENCY_KEY_HEADER, "large-1"))
                .andExpect(status().isConflict())
                .andExpect(header().string("Idempotency-Reject-Reason", "response_unavailable"));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void aResponseWithinTheLimitIsStillReplayed() throws Exception {
        mockMvc.perform(post("/small").header(Idempotent.IDEMPOTENCY_KEY_HEADER, "small-1"))
                .andExpect(status().isCreated())
                .andExpect(content().string("small"));

        mockMvc.perform(post("/small").header(Idempotent.IDEMPOTENCY_KEY_HEADER, "small-1"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"))
                .andExpect(content().string("small"));
    }

    @Configuration
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean
        AtomicInteger handlerInvocations() {
            return new AtomicInteger();
        }

        @Bean(InMemoryIdempotencyStore.QUALIFIER)
        IdempotencyStore inMemoryIdempotencyStore() {
            return new InMemoryIdempotencyStore();
        }

        @Bean
        LargeController largeController(AtomicInteger handlerInvocations) {
            return new LargeController(handlerInvocations);
        }
    }

    @RestController
    static class LargeController {

        private final AtomicInteger handlerInvocations;

        LargeController(AtomicInteger handlerInvocations) {
            this.handlerInvocations = handlerInvocations;
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @PostMapping("/large")
        ResponseEntity<String> large() {
            handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(LARGE_BODY);
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @PostMapping("/small")
        ResponseEntity<String> small() {
            return ResponseEntity.status(HttpStatus.CREATED).body("small");
        }
    }
}
