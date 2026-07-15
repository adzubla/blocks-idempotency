package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.annotation.Idempotent.OnStoreFailure;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.cookie;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end MockMvc coverage of Slices 001-012: an {@code @Idempotent}
 * handler intercepted through the real filter + interceptor + engine +
 * in-memory store, over HTTP.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = IdempotencyEndToEndTest.TestApplication.class,
        properties = "idempotency.default-store=" + InMemoryIdempotencyStore.QUALIFIER)
@AutoConfigureMockMvc
class IdempotencyEndToEndTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AtomicInteger handlerInvocations;

    @Autowired
    private IdempotencyStore idempotencyStore;

    @BeforeEach
    void resetFixtures() {
        handlerInvocations.set(0);
        ((InMemoryIdempotencyStore) idempotencyStore).setUnavailable(false);
    }

    @Test
    void firstRequestExecutesTheHandlerAndCachesThe2xxResponse() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(1));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void repeatWithSameKeyReplaysWithoutReExecutingTheHandler() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value(1));

        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"))
                .andExpect(jsonPath("$.orderId").value(1));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void requestsWithDifferentKeysAreIsolated() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-3")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value(1));

        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-4")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(2));

        assertThat(handlerInvocations.get()).isEqualTo(2);
    }

    @Test
    void sameKeyWithADifferentBodyIsRejectedWith422() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-6")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value(1));

        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-6")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":20}"))
                .andExpect(status().isUnprocessableContent())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(header().string("Idempotency-Reject-Reason", "collision"));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void sameKeyWithACosmeticallyReorderedBodyStillReplays() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-7")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10,\"currency\":\"USD\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value(1));

        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-7")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"currency\":\"USD\",\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"))
                .andExpect(jsonPath("$.orderId").value(1));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void concurrentDuplicateWhileInProgressIsRejectedWith409() throws Exception {
        String body = "{\"amount\":10}";
        EffectiveKey key = new EffectiveKey("POST", "/orders", "", "key-8");
        String fingerprint = Fingerprint.sha256(key.method(), key.path(), body.getBytes(StandardCharsets.UTF_8));
        idempotencyStore.reserve(key, fingerprint, Duration.ofSeconds(30));

        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-8")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isConflict())
                .andExpect(header().exists("Retry-After"))
                .andExpect(header().string("Idempotency-Reject-Reason", "in_progress"));

        assertThat(handlerInvocations.get()).isZero();
    }

    @Test
    void nonSuccessResponseReleasesTheKeyAndARetryReExecutes() throws Exception {
        mockMvc.perform(post("/flaky")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-flaky-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"mode\":\"error\"}"))
                .andExpect(status().isInternalServerError());

        assertThat(handlerInvocations.get()).isEqualTo(1);

        mockMvc.perform(post("/flaky")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-flaky-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"mode\":\"ok\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(2));

        assertThat(handlerInvocations.get()).isEqualTo(2);
    }

    @Test
    void thrownExceptionReleasesTheKeyAndARetryReExecutes() throws Exception {
        // No error-page/exception-resolver machinery is wired into this minimal
        // test app, so the exception propagates out of perform() rather than
        // being translated to a 500 response - what matters here is that the
        // interceptor's afterCompletion still ran and released the key.
        assertThatThrownBy(() -> mockMvc.perform(post("/flaky")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-flaky-2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"mode\":\"throw\"}")))
                .hasRootCauseInstanceOf(IllegalStateException.class);

        assertThat(handlerInvocations.get()).isEqualTo(1);

        mockMvc.perform(post("/flaky")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-flaky-2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"mode\":\"ok\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(2));

        assertThat(handlerInvocations.get()).isEqualTo(2);
    }

    @Test
    void missingHeaderWithKeyRequiredIsRejectedWith400() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Idempotency-Reject-Reason", "key_required"));

        assertThat(handlerInvocations.get()).isZero();
    }

    @Test
    void headerKeyOverTheConfiguredMaxLengthIsRejectedWith400() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "k".repeat(256))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Idempotency-Reject-Reason", "key_invalid"));

        assertThat(handlerInvocations.get()).isZero();
    }

    @Test
    void headerKeyOutsideTheAllowedCharsetIsRejectedWith400() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "not a valid key!")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Idempotency-Reject-Reason", "key_invalid"));

        assertThat(handlerInvocations.get()).isZero();
    }

    @Test
    void missingHeaderWithKeyOptionalPassesThroughUnprotected() throws Exception {
        mockMvc.perform(post("/optional-orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(1));

        // No record was created: a second unkeyed call re-executes rather than
        // replaying or colliding.
        mockMvc.perform(post("/optional-orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(2));

        assertThat(handlerInvocations.get()).isEqualTo(2);
    }

    @Test
    void presentHeaderWithKeyOptionalBehavesAsSlice001() throws Exception {
        mockMvc.perform(post("/optional-orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-optional-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(1));

        mockMvc.perform(post("/optional-orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-optional-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"))
                .andExpect(jsonPath("$.orderId").value(1));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void explicitTtlOverridesTheGlobalDefaultAndFlowsThroughToTheStore() throws Exception {
        RecordingIdempotencyStore recordingStore = (RecordingIdempotencyStore) idempotencyStore;

        mockMvc.perform(post("/orders-custom-ttl")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-custom-ttl")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated());

        // ttl = "PT2H" on the endpoint, well short of the 24h global default -
        // proves PolicyResolver's resolved ttl (not the global default) is what
        // reaches engine.complete()/store.complete().
        assertThat(recordingStore.lastCompletedTtl()).isEqualTo(Duration.ofHours(2));
    }

    @Test
    void storeUnavailableWithFailOpenLetsTheHandlerRunUnprotected() throws Exception {
        ((InMemoryIdempotencyStore) idempotencyStore).setUnavailable(true);

        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-outage-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(1));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void storeUnavailableWithFailClosedReturns503WithoutExecutingTheHandler() throws Exception {
        ((InMemoryIdempotencyStore) idempotencyStore).setUnavailable(true);

        mockMvc.perform(post("/orders-fail-closed")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-outage-2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Idempotency-Reject-Reason", "store_unavailable"));

        assertThat(handlerInvocations.get()).isZero();
    }

    @Test
    void firstRequestExecutesAndCachesViaTheBodyFieldStrategy() throws Exception {
        mockMvc.perform(post("/orders-by-field")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"order-1\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(1));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void repeatWithTheSameFieldValueReplaysViaTheBodyFieldStrategy() throws Exception {
        mockMvc.perform(post("/orders-by-field")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"order-2\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value(1));

        mockMvc.perform(post("/orders-by-field")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"order-2\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"))
                .andExpect(jsonPath("$.orderId").value(1));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void differentFieldValuesAreIsolatedViaTheBodyFieldStrategy() throws Exception {
        mockMvc.perform(post("/orders-by-field")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"order-3\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value(1));

        mockMvc.perform(post("/orders-by-field")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"order-4\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(2));

        assertThat(handlerInvocations.get()).isEqualTo(2);
    }

    @Test
    void missingFieldWithKeyRequiredIsRejectedWith400() throws Exception {
        mockMvc.perform(post("/orders-by-field")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"other\":\"value\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Idempotency-Reject-Reason", "key_required"));

        assertThat(handlerInvocations.get()).isZero();
    }

    @Test
    void missingFieldWithKeyOptionalPassesThroughUnprotected() throws Exception {
        mockMvc.perform(post("/orders-by-field-optional")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"other\":\"value\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(1));

        // No record was created: a second identical call re-executes rather
        // than replaying or colliding.
        mockMvc.perform(post("/orders-by-field-optional")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"other\":\"value\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(jsonPath("$.orderId").value(2));

        assertThat(handlerInvocations.get()).isEqualTo(2);
    }

    @Test
    void nonJsonBodyOnAFieldPathEndpointIsRejectedWith400NotA500() throws Exception {
        mockMvc.perform(post("/orders-by-field")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("not json"))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Idempotency-Reject-Reason", "key_required"));

        assertThat(handlerInvocations.get()).isZero();
    }

    @Test
    void aNonScalarJsonPathMatchIsRejectedWith400() throws Exception {
        mockMvc.perform(post("/orders-by-field")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":{\"nested\":\"value\"}}"))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Idempotency-Reject-Reason", "key_required"));

        assertThat(handlerInvocations.get()).isZero();
    }

    @Test
    void sameFieldValueWithADifferentBodyIsRejectedWith422ViaTheBodyFieldStrategy() throws Exception {
        mockMvc.perform(post("/orders-by-field")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"order-5\",\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value(1));

        mockMvc.perform(post("/orders-by-field")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"order-5\",\"amount\":20}"))
                .andExpect(status().isUnprocessableContent())
                .andExpect(header().doesNotExist("Idempotency-Replayed"))
                .andExpect(header().string("Idempotency-Reject-Reason", "collision"));

        assertThat(handlerInvocations.get()).isEqualTo(1);
    }

    @Test
    void replayedResponseNeverCarriesSetCookie() throws Exception {
        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-5")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(cookie().exists("session"));

        mockMvc.perform(post("/orders")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-5")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"))
                .andExpect(cookie().doesNotExist("session"));
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
            return new RecordingIdempotencyStore();
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
            return ResponseEntity.status(HttpStatus.CREATED)
                    .header(HttpHeaders.SET_COOKIE, "session=abc123")
                    .body(Map.of("orderId", orderId));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, keyRequired = false)
        @PostMapping("/optional-orders")
        ResponseEntity<Map<String, Object>> createOptionalOrder(@RequestBody Map<String, Object> body) {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, ttl = "PT2H")
        @PostMapping("/orders-custom-ttl")
        ResponseEntity<Map<String, Object>> createOrderWithCustomTtl(@RequestBody Map<String, Object> body) {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, onStoreFailure = OnStoreFailure.CLOSED)
        @PostMapping("/orders-fail-closed")
        ResponseEntity<Map<String, Object>> createOrderFailClosed(@RequestBody Map<String, Object> body) {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }

        @Idempotent(fieldPath = "$.orderId")
        @PostMapping("/orders-by-field")
        ResponseEntity<Map<String, Object>> createOrderByField(@RequestBody Map<String, Object> body) {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }

        @Idempotent(fieldPath = "$.orderId", keyRequired = false)
        @PostMapping("/orders-by-field-optional")
        ResponseEntity<Map<String, Object>> createOrderByFieldOptional(@RequestBody Map<String, Object> body) {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @PostMapping("/flaky")
        ResponseEntity<Map<String, Object>> createFlaky(@RequestBody Map<String, Object> body) {
            int orderId = handlerInvocations.incrementAndGet();
            String mode = (String) body.get("mode");
            if ("error".equals(mode)) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", "boom"));
            }
            if ("throw".equals(mode)) {
                throw new IllegalStateException("boom");
            }
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }
    }

    /** Records the {@code responseTtl} passed to {@link #complete}, so tests can assert on it. */
    static class RecordingIdempotencyStore extends InMemoryIdempotencyStore {

        private volatile Duration lastCompletedTtl;

        @Override
        public void complete(EffectiveKey key, String fenceToken, CachedResponse response, Duration responseTtl) {
            this.lastCompletedTtl = responseTtl;
            super.complete(key, fenceToken, response, responseTtl);
        }

        Duration lastCompletedTtl() {
            return lastCompletedTtl;
        }
    }
}
