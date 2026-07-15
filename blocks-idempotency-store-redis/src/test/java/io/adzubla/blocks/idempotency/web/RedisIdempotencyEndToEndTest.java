package io.adzubla.blocks.idempotency.web;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.redis.RedisIdempotencyStore;
import org.junit.jupiter.api.BeforeEach;
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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end MockMvc coverage of {@code @Idempotent(store = "redis")} over a
 * real Testcontainers Redis: the interceptor + engine + {@link
 * RedisIdempotencyStore}, driven over HTTP, proving
 * Slice 015's happy-path acceptance criterion. Unlike the Postgres store
 * there's no transaction-participation question here (ADR 0001: Redis is a
 * standalone best-effort cache) - the handler just tracks its own invocation
 * count, no database involved.
 *
 * <p>{@code com.redis:testcontainers-redis} has no Spring Boot {@code
 * @ServiceConnection} factory (unlike {@code PostgreSQLContainer}), so the
 * connection is wired via {@code @DynamicPropertySource} against {@code
 * spring.data.redis.*} instead - Boot's own {@code DataRedisAutoConfiguration}
 * then builds the {@code StringRedisTemplate} from that as usual.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, classes = RedisIdempotencyEndToEndTest.TestApplication.class)
@AutoConfigureMockMvc
class RedisIdempotencyEndToEndTest {

    @Container
    private static final RedisContainer REDIS = new RedisContainer("redis:7-alpine");

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getRedisHost);
        registry.add("spring.data.redis.port", REDIS::getRedisPort);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AtomicInteger handlerInvocations;

    @BeforeEach
    void resetFixtures() {
        handlerInvocations.set(0);
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
        @PostMapping("/orders")
        ResponseEntity<Map<String, Object>> createOrder(@RequestBody Map<String, Object> body) {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
        }
    }
}
