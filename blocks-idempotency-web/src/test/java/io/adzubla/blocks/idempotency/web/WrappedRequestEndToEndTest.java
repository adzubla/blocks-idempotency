package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import jakarta.servlet.Filter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * A filter ordered after {@link IdempotencyFilter} that wraps the request and
 * response (as Spring Security's filter chain does) must not stop the
 * interceptor from finding the buffered body and the captured response.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = WrappedRequestEndToEndTest.TestApplication.class,
        properties = "idempotency.default-store=" + InMemoryIdempotencyStore.QUALIFIER)
@AutoConfigureMockMvc
class WrappedRequestEndToEndTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AtomicInteger handlerInvocations;

    @Test
    void repeatReplaysEvenWhenALaterFilterWrapsRequestAndResponse() throws Exception {
        mockMvc.perform(post("/orders").header(Idempotent.IDEMPOTENCY_KEY_HEADER, "wrapped-1")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"amount\":10}"))
                .andExpect(status().isCreated());
        int invocationsAfterFirst = handlerInvocations.get();

        mockMvc.perform(post("/orders").header(Idempotent.IDEMPOTENCY_KEY_HEADER, "wrapped-1")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"amount\":10}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"))
                .andExpect(jsonPath("$.orderId").value(invocationsAfterFirst));

        assertThat(handlerInvocations.get()).isEqualTo(invocationsAfterFirst);
    }

    @Test
    void differentBodyWithTheSameKeyIsStillACollision() throws Exception {
        mockMvc.perform(post("/orders").header(Idempotent.IDEMPOTENCY_KEY_HEADER, "wrapped-2")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"amount\":10}"))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/orders").header(Idempotent.IDEMPOTENCY_KEY_HEADER, "wrapped-2")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"amount\":99}"))
                .andExpect(status().isUnprocessableContent())
                .andExpect(header().string("Idempotency-Reject-Reason", "collision"));
    }

    @Test
    void bodyFieldKeyStillResolvesThroughTheWrapper() throws Exception {
        mockMvc.perform(post("/orders-by-field").contentType(MediaType.APPLICATION_JSON).content("{\"orderId\":\"o-1\"}"))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/orders-by-field").contentType(MediaType.APPLICATION_JSON).content("{\"orderId\":\"o-1\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotency-Replayed", "true"));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean(InMemoryIdempotencyStore.QUALIFIER)
        IdempotencyStore idempotencyStore() {
            return new InMemoryIdempotencyStore();
        }

        @Bean
        AtomicInteger handlerInvocations() {
            return new AtomicInteger();
        }

        @Bean
        OrdersController ordersController(AtomicInteger handlerInvocations) {
            return new OrdersController(handlerInvocations);
        }

        /** Stands in for any filter between {@link IdempotencyFilter} and the dispatcher that wraps both objects. */
        @Bean
        FilterRegistrationBean<Filter> wrappingFilter() {
            Filter filter = (request, response, chain) -> chain.doFilter(
                    new HttpServletRequestWrapper((HttpServletRequest) request),
                    new HttpServletResponseWrapper((HttpServletResponse) response));
            FilterRegistrationBean<Filter> registration = new FilterRegistrationBean<>(filter);
            registration.setOrder(0);
            return registration;
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
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", handlerInvocations.incrementAndGet()));
        }

        @Idempotent(fieldPath = "$.orderId")
        @PostMapping("/orders-by-field")
        ResponseEntity<Map<String, Object>> createOrderByField(@RequestBody Map<String, Object> body) {
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", handlerInvocations.incrementAndGet()));
        }
    }
}
