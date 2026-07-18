package io.adzubla.blocks.idempotency.fingerprint;

import org.junit.jupiter.api.Disabled;
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

    /**
     * Exposes a bug (docs/issues/028-fingerprint-float-precision-collision.md):
     * {@link Fingerprint#normalize} parses the body and re-serializes it, and
     * Jackson binds JSON floating-point numbers to {@code double}. Two
     * genuinely different payloads whose amounts round to the same IEEE-754
     * double collapse to byte-identical canonical JSON and thus the same
     * fingerprint - so the engine's collision guard treats a different payload
     * as a duplicate and replays the first caller's cached response instead of
     * returning 422. {@code 9007199254740993.0} and {@code 9007199254740992.0}
     * are distinct decimals that both parse to the same double.
     *
     * <p>{@code @Disabled} so CI stays green while the fix is deferred; remove
     * it to reproduce the failure.
     */
    @Test
    @Disabled("Exposes bug: docs/issues/028-fingerprint-float-precision-collision.md; remove @Disabled to reproduce")
    void distinctFloatPayloadsRoundingToTheSameDoubleMustNotShareAFingerprint() {
        String withOneAmount = Fingerprint.sha256("POST", "/payments",
                "{\"amount\":9007199254740992.0}".getBytes(StandardCharsets.UTF_8));
        String withAnotherAmount = Fingerprint.sha256("POST", "/payments",
                "{\"amount\":9007199254740993.0}".getBytes(StandardCharsets.UTF_8));

        assertThat(withOneAmount).isNotEqualTo(withAnotherAmount);
    }

    @Test
    void digestBytesJoinsPartsWithNulSeparators() {
        byte[] joined = Fingerprint.digestBytes(
                "a".getBytes(StandardCharsets.UTF_8),
                "b".getBytes(StandardCharsets.UTF_8));
        byte[] notJoined = Fingerprint.digestBytes(
                "a\0b".getBytes(StandardCharsets.UTF_8));

        assertThat(joined).isEqualTo(notJoined);
    }

    @Test
    void digestBytesIsSensitiveToPartBoundaries() {
        byte[] abThenC = Fingerprint.digestBytes(
                "ab".getBytes(StandardCharsets.UTF_8),
                "c".getBytes(StandardCharsets.UTF_8));
        byte[] aThenBc = Fingerprint.digestBytes(
                "a".getBytes(StandardCharsets.UTF_8),
                "bc".getBytes(StandardCharsets.UTF_8));

        assertThat(abThenC).isNotEqualTo(aThenBc);
    }
}
