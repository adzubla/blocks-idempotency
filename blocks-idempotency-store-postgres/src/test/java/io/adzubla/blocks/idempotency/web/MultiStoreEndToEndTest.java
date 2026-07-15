package io.adzubla.blocks.idempotency.web;

import com.redis.testcontainers.RedisContainer;
import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.postgres.PostgresIdempotencyStore;
import io.adzubla.blocks.idempotency.store.redis.RedisIdempotencyStore;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Proves an application can register both store modules at once and have each
 * {@code @Idempotent} endpoint route to the store its own {@code store}
 * attribute names, rather than a single application-wide store. Both
 * auto-configurations are on the classpath here (Redis and Postgres); if
 * {@link io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry} didn't
 * exist, wiring a single unqualified {@code IdempotencyStore} bean into one
 * engine would fail context startup with {@code NoUniqueBeanDefinitionException}
 * the moment both store beans were present.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, classes = MultiStoreEndToEndTest.TestApplication.class)
@AutoConfigureMockMvc
class MultiStoreEndToEndTest {

    @Container
    @ServiceConnection
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

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

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void resetFixtures() {
        handlerInvocations.set(0);
        jdbc.update("DELETE FROM idempotency_record");
    }

    @Test
    void aRedisBackedEndpointAndAPostgresBackedEndpointBothWorkInTheSameApplication() throws Exception {
        mockMvc.perform(post("/orders-redis")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-redis-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"));

        mockMvc.perform(post("/orders-redis")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-redis-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"));

        mockMvc.perform(post("/orders-postgres")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-postgres-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"));

        mockMvc.perform(post("/orders-postgres")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "key-postgres-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"));

        // Two handler executions total (one per store-backed endpoint), each
        // key replayed exactly once - proving the two stores operate
        // independently rather than one shadowing the other.
        assertThat(handlerInvocations.get()).isEqualTo(2);
    }

    @Test
    void theSameKeyValueOnDifferentStoreBackedEndpointsDoesNotCollide() throws Exception {
        mockMvc.perform(post("/orders-redis")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "shared-key")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"));

        mockMvc.perform(post("/orders-postgres")
                        .header(Idempotent.IDEMPOTENCY_KEY_HEADER, "shared-key")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotency-Replayed"));

        assertThat(handlerInvocations.get()).isEqualTo(2);
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
        @PostMapping("/orders-redis")
        ResponseEntity<Map<String, Object>> createOrderViaRedis() {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId, "store", RedisIdempotencyStore.QUALIFIER));
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER, store = PostgresIdempotencyStore.QUALIFIER)
        @PostMapping("/orders-postgres")
        ResponseEntity<Map<String, Object>> createOrderViaPostgres() {
            int orderId = handlerInvocations.incrementAndGet();
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId, "store", PostgresIdempotencyStore.QUALIFIER));
        }
    }
}
