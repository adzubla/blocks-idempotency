package io.adzubla.blocks.idempotency.fingerprint;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class FingerprintTest {

    @Test
    void sameInputsProduceTheSameFingerprint() {
        byte[] body = "{\"amount\":10}".getBytes(StandardCharsets.UTF_8);

        String first = Fingerprint.sha256("/orders", "POST", body);
        String second = Fingerprint.sha256("/orders", "POST", body);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void differentBodiesProduceDifferentFingerprints() {
        String withOneBody = Fingerprint.sha256("/orders", "POST", "{\"amount\":10}".getBytes(StandardCharsets.UTF_8));
        String withAnotherBody = Fingerprint.sha256("/orders", "POST", "{\"amount\":20}".getBytes(StandardCharsets.UTF_8));

        assertThat(withOneBody).isNotEqualTo(withAnotherBody);
    }

    @Test
    void differentPathsProduceDifferentFingerprints() {
        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);

        String orders = Fingerprint.sha256("/orders", "POST", body);
        String invoices = Fingerprint.sha256("/invoices", "POST", body);

        assertThat(orders).isNotEqualTo(invoices);
    }

    @Test
    void cosmeticKeyReorderingDoesNotChangeTheFingerprint() {
        String inOrder = Fingerprint.sha256("/orders", "POST",
                "{\"amount\":10,\"currency\":\"USD\"}".getBytes(StandardCharsets.UTF_8));
        String reordered = Fingerprint.sha256("/orders", "POST",
                "{\"currency\":\"USD\",\"amount\":10}".getBytes(StandardCharsets.UTF_8));

        assertThat(inOrder).isEqualTo(reordered);
    }

    @Test
    void nestedKeyReorderingDoesNotChangeTheFingerprint() {
        String inOrder = Fingerprint.sha256("/orders", "POST",
                "{\"amount\":10,\"customer\":{\"id\":1,\"name\":\"a\"}}".getBytes(StandardCharsets.UTF_8));
        String reordered = Fingerprint.sha256("/orders", "POST",
                "{\"customer\":{\"name\":\"a\",\"id\":1},\"amount\":10}".getBytes(StandardCharsets.UTF_8));

        assertThat(inOrder).isEqualTo(reordered);
    }

    @Test
    void emptyBodyIsHandled() {
        String first = Fingerprint.sha256("/orders", "POST", new byte[0]);
        String second = Fingerprint.sha256("/orders", "POST", new byte[0]);

        assertThat(first).isEqualTo(second);
    }

    /**
     * Regression for docs/issues/028-fingerprint-float-precision-collision.md:
     * two genuinely different payloads whose floats round to the same IEEE-754
     * double ({@code 9007199254740992.0} and {@code 9007199254740993.0}) must
     * not share a fingerprint - otherwise a different payload would replay the
     * first caller's cached response instead of being flagged as a collision.
     * Fixed by reading floats as exact {@link java.math.BigDecimal} rather than
     * lossy {@code double}.
     */
    @Test
    void distinctFloatPayloadsRoundingToTheSameDoubleMustNotShareAFingerprint() {
        String withOneAmount = Fingerprint.sha256("/payments", "POST",
                "{\"amount\":9007199254740992.0}".getBytes(StandardCharsets.UTF_8));
        String withAnotherAmount = Fingerprint.sha256("/payments", "POST",
                "{\"amount\":9007199254740993.0}".getBytes(StandardCharsets.UTF_8));

        assertThat(withOneAmount).isNotEqualTo(withAnotherAmount);
    }

    /**
     * The reverse of the precision fix: numbers are compared by their exact
     * JSON text, so the integer and float spellings of the same value are
     * distinct fingerprints (a conservative choice - a differing byte payload
     * is treated as a collision, never silently merged).
     */
    @Test
    void integerAndFloatSpellingsOfTheSameValueProduceDifferentFingerprints() {
        String asInteger = Fingerprint.sha256("/payments", "POST",
                "{\"amount\":1}".getBytes(StandardCharsets.UTF_8));
        String asFloat = Fingerprint.sha256("/payments", "POST",
                "{\"amount\":1.0}".getBytes(StandardCharsets.UTF_8));

        assertThat(asInteger).isNotEqualTo(asFloat);
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
