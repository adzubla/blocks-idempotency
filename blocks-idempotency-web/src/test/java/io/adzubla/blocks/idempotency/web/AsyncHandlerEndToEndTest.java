package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.InMemoryIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Over a real servlet container: handlers that complete on another thread
 * ({@code CompletableFuture}, {@code StreamingResponseBody}) must get their
 * response to the client whether or not they are {@code @Idempotent}, and an
 * {@code @Idempotent} one must be reserved once, completed with the async
 * result, and replayed on a repeat.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = AsyncHandlerEndToEndTest.TestApplication.class,
        properties = "idempotency.default-store=" + InMemoryIdempotencyStore.QUALIFIER)
class AsyncHandlerEndToEndTest {

    @Value("${local.server.port}")
    private int port;

    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void asyncHandlerThatIsNotIdempotentStillReturnsItsBody() throws Exception {
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder(uri("/async")).GET().build(), HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("async-body");
    }

    @Test
    void streamingHandlerThatIsNotIdempotentStillReturnsItsBody() throws Exception {
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder(uri("/stream")).GET().build(), HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("chunk-1chunk-2");
    }

    @Test
    void idempotentAsyncHandlerExecutesOnceAndReplaysItsAsyncResult() throws Exception {
        HttpResponse<String> first = post("/async-orders", "{\"mode\":\"ok\"}", "async-1");
        HttpResponse<String> repeat = post("/async-orders", "{\"mode\":\"ok\"}", "async-1");

        assertThat(first.statusCode()).isEqualTo(201);
        assertThat(first.body()).startsWith("{\"orderId\":");
        assertThat(first.headers().firstValue("Idempotency-Replayed")).isEmpty();
        assertThat(repeat.statusCode()).isEqualTo(201);
        assertThat(repeat.body()).isEqualTo(first.body());
        assertThat(repeat.headers().firstValue("Idempotency-Replayed")).contains("true");
    }

    @Test
    void idempotentAsyncHandlerThatFailsReleasesTheKeyForARetry() throws Exception {
        HttpResponse<String> failed = post("/async-orders", "{\"mode\":\"fail\"}", "async-2");
        HttpResponse<String> retry = post("/async-orders", "{\"mode\":\"fail\"}", "async-2");

        assertThat(failed.statusCode()).isEqualTo(500);
        // Re-executed (and failed again) rather than rejected as in-progress or replayed.
        assertThat(retry.statusCode()).isEqualTo(500);
        assertThat(retry.headers().firstValue("Idempotency-Reject-Reason")).isEmpty();
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private HttpResponse<String> post(String path, String json, String key) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .header(Idempotent.IDEMPOTENCY_KEY_HEADER, key)
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean(InMemoryIdempotencyStore.QUALIFIER)
        IdempotencyStore idempotencyStore() {
            return new InMemoryIdempotencyStore();
        }

        @Bean
        AsyncController asyncController() {
            return new AsyncController();
        }
    }

    @RestController
    static class AsyncController {

        private final AtomicInteger invocations = new AtomicInteger();

        @GetMapping("/async")
        CompletableFuture<String> async() {
            return CompletableFuture.supplyAsync(() -> "async-body");
        }

        @GetMapping("/stream")
        StreamingResponseBody stream() {
            return output -> {
                output.write("chunk-1".getBytes(StandardCharsets.UTF_8));
                output.flush();
                output.write("chunk-2".getBytes(StandardCharsets.UTF_8));
            };
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @PostMapping("/async-orders")
        CompletableFuture<ResponseEntity<Map<String, Object>>> createOrder(@RequestBody Map<String, Object> body) {
            return CompletableFuture.supplyAsync(() -> {
                int orderId = invocations.incrementAndGet();
                if ("fail".equals(body.get("mode"))) {
                    throw new IllegalStateException("boom");
                }
                return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("orderId", orderId));
            });
        }
    }
}
