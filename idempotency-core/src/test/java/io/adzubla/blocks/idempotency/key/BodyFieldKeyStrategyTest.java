package io.adzubla.blocks.idempotency.key;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class BodyFieldKeyStrategyTest {

    @Test
    void resolvesAValueAtATopLevelPath() {
        byte[] body = "{\"orderId\":\"order-123\"}".getBytes(StandardCharsets.UTF_8);

        assertThat(BodyFieldKeyStrategy.resolve(body, "$.orderId")).contains("order-123");
    }

    @Test
    void resolvesAValueAtANestedPath() {
        byte[] body = "{\"order\":{\"id\":\"order-456\"}}".getBytes(StandardCharsets.UTF_8);

        assertThat(BodyFieldKeyStrategy.resolve(body, "$.order.id")).contains("order-456");
    }

    @Test
    void coercesANumericMatchToItsStringForm() {
        byte[] body = "{\"orderId\":12345}".getBytes(StandardCharsets.UTF_8);

        assertThat(BodyFieldKeyStrategy.resolve(body, "$.orderId")).contains("12345");
    }

    @Test
    void coercesABooleanMatchToItsStringForm() {
        byte[] body = "{\"confirmed\":true}".getBytes(StandardCharsets.UTF_8);

        assertThat(BodyFieldKeyStrategy.resolve(body, "$.confirmed")).contains("true");
    }

    @Test
    void isEmptyWhenTheFieldIsMissing() {
        byte[] body = "{\"other\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        assertThat(BodyFieldKeyStrategy.resolve(body, "$.orderId")).isEmpty();
    }

    @Test
    void isEmptyWhenTheBodyIsNotValidJson() {
        byte[] body = "not json".getBytes(StandardCharsets.UTF_8);

        assertThat(BodyFieldKeyStrategy.resolve(body, "$.orderId")).isEmpty();
    }

    @Test
    void isEmptyWhenTheBodyIsEmpty() {
        assertThat(BodyFieldKeyStrategy.resolve(new byte[0], "$.orderId")).isEmpty();
    }

    @Test
    void isEmptyWhenTheMatchIsAJsonObject() {
        byte[] body = "{\"order\":{\"id\":\"order-1\"}}".getBytes(StandardCharsets.UTF_8);

        assertThat(BodyFieldKeyStrategy.resolve(body, "$.order")).isEmpty();
    }

    @Test
    void isEmptyWhenTheMatchIsAJsonArray() {
        byte[] body = "{\"items\":[1,2,3]}".getBytes(StandardCharsets.UTF_8);

        assertThat(BodyFieldKeyStrategy.resolve(body, "$.items")).isEmpty();
    }
}
