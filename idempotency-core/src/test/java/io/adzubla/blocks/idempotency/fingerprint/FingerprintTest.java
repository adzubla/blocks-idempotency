package io.adzubla.blocks.idempotency.fingerprint;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class FingerprintTest {

    @Test
    void sameInputsProduceTheSameFingerprint() {
        byte[] body = "{\"amount\":10}".getBytes(StandardCharsets.UTF_8);

        String first = Fingerprint.sha256("POST", "/orders", body);
        String second = Fingerprint.sha256("POST", "/orders", body);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void differentBodiesProduceDifferentFingerprints() {
        String withOneBody = Fingerprint.sha256("POST", "/orders", "{\"amount\":10}".getBytes(StandardCharsets.UTF_8));
        String withAnotherBody = Fingerprint.sha256("POST", "/orders", "{\"amount\":20}".getBytes(StandardCharsets.UTF_8));

        assertThat(withOneBody).isNotEqualTo(withAnotherBody);
    }

    @Test
    void differentPathsProduceDifferentFingerprints() {
        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);

        String orders = Fingerprint.sha256("POST", "/orders", body);
        String invoices = Fingerprint.sha256("POST", "/invoices", body);

        assertThat(orders).isNotEqualTo(invoices);
    }

    @Test
    void cosmeticKeyReorderingDoesNotChangeTheFingerprint() {
        String inOrder = Fingerprint.sha256("POST", "/orders",
                "{\"amount\":10,\"currency\":\"USD\"}".getBytes(StandardCharsets.UTF_8));
        String reordered = Fingerprint.sha256("POST", "/orders",
                "{\"currency\":\"USD\",\"amount\":10}".getBytes(StandardCharsets.UTF_8));

        assertThat(inOrder).isEqualTo(reordered);
    }

    @Test
    void nestedKeyReorderingDoesNotChangeTheFingerprint() {
        String inOrder = Fingerprint.sha256("POST", "/orders",
                "{\"amount\":10,\"customer\":{\"id\":1,\"name\":\"a\"}}".getBytes(StandardCharsets.UTF_8));
        String reordered = Fingerprint.sha256("POST", "/orders",
                "{\"customer\":{\"name\":\"a\",\"id\":1},\"amount\":10}".getBytes(StandardCharsets.UTF_8));

        assertThat(inOrder).isEqualTo(reordered);
    }

    @Test
    void emptyBodyIsHandled() {
        String first = Fingerprint.sha256("POST", "/orders", new byte[0]);
        String second = Fingerprint.sha256("POST", "/orders", new byte[0]);

        assertThat(first).isEqualTo(second);
    }
}
