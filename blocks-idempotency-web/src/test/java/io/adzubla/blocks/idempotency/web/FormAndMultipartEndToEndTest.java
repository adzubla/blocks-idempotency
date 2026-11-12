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
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Over a real servlet container (MockMvc keeps parameters apart from the body,
 * so it can't show this): {@link IdempotencyFilter} sits in front of every
 * request, and must leave form and multipart bodies for the container to
 * parse - on endpoints that aren't {@code @Idempotent} and on ones that are.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = FormAndMultipartEndToEndTest.TestApplication.class,
        properties = "idempotency.default-store=" + InMemoryIdempotencyStore.QUALIFIER)
class FormAndMultipartEndToEndTest {

    private static final String FORM = "application/x-www-form-urlencoded";

    @Value("${local.server.port}")
    private int port;

    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void formPostToANonIdempotentEndpointStillSeesItsParameters() throws Exception {
        HttpResponse<String> response = post("/form", FORM, "name=bob", null);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("hello bob");
    }

    @Test
    void multipartUploadToANonIdempotentEndpointStillSeesItsParts() throws Exception {
        String boundary = "----idempotency-test-boundary";
        String body = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n"
                + "Content-Type: text/plain\r\n\r\n"
                + "file-content\r\n"
                + "--" + boundary + "--\r\n";

        HttpResponse<String> response = post("/upload", "multipart/form-data; boundary=" + boundary, body, null);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("uploaded file-content");
    }

    @Test
    void idempotentFormPostExecutesOnceAndReplays() throws Exception {
        HttpResponse<String> first = post("/idempotent-form", FORM, "name=bob", "form-1");
        HttpResponse<String> repeat = post("/idempotent-form", FORM, "name=bob", "form-1");

        assertThat(first.statusCode()).isEqualTo(200);
        assertThat(first.body()).startsWith("hello bob #");
        assertThat(repeat.statusCode()).isEqualTo(200);
        assertThat(repeat.body()).isEqualTo(first.body());
        assertThat(repeat.headers().firstValue("Idempotency-Replayed")).contains("true");
    }

    @Test
    void idempotentFormPostWithDifferentParametersIsACollision() throws Exception {
        post("/idempotent-form", FORM, "name=bob", "form-2");

        HttpResponse<String> different = post("/idempotent-form", FORM, "name=alice", "form-2");

        assertThat(different.statusCode()).isEqualTo(422);
        assertThat(different.headers().firstValue("Idempotency-Reject-Reason")).contains("collision");
    }

    private HttpResponse<String> post(String path, String contentType, String body, String key) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (key != null) {
            request.header(Idempotent.IDEMPOTENCY_KEY_HEADER, key);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean(InMemoryIdempotencyStore.QUALIFIER)
        IdempotencyStore idempotencyStore() {
            return new InMemoryIdempotencyStore();
        }

        @Bean
        FormController formController() {
            return new FormController();
        }
    }

    @RestController
    static class FormController {

        private final AtomicInteger invocations = new AtomicInteger();

        @PostMapping("/form")
        String form(@RequestParam("name") String name) {
            return "hello " + name;
        }

        @PostMapping("/upload")
        String upload(@RequestParam("file") MultipartFile file) throws IOException {
            return "uploaded " + new String(file.getBytes(), StandardCharsets.UTF_8);
        }

        @Idempotent(header = Idempotent.IDEMPOTENCY_KEY_HEADER)
        @PostMapping("/idempotent-form")
        String idempotentForm(@RequestParam("name") String name) {
            return "hello " + name + " #" + invocations.incrementAndGet();
        }
    }
}
