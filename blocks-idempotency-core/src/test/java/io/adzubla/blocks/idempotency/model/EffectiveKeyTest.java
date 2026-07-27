package io.adzubla.blocks.idempotency.model;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;

class EffectiveKeyTest {

    @Test
    void sameFieldsProduceTheSameDigest() {
        EffectiveKey first = new EffectiveKey("/orders", "POST", "user-1", "abc");
        EffectiveKey second = new EffectiveKey("/orders", "POST", "user-1", "abc");

        assertThat(first.digestBytes()).isEqualTo(second.digestBytes());
    }

    @Test
    void differentPrincipalsProduceDifferentDigests() {
        EffectiveKey first = new EffectiveKey("/orders", "POST", "user-1", "abc");
        EffectiveKey second = new EffectiveKey("/orders", "POST", "user-2", "abc");

        assertThat(first.digestBytes()).isNotEqualTo(second.digestBytes());
    }

    @Test
    void differentValuesProduceDifferentDigests() {
        EffectiveKey first = new EffectiveKey("/orders", "POST", "user-1", "abc");
        EffectiveKey second = new EffectiveKey("/orders", "POST", "user-1", "xyz");

        assertThat(first.digestBytes()).isNotEqualTo(second.digestBytes());
    }

    @Test
    void digestMatchesTheNulSeparatedSha256Shape() throws NoSuchAlgorithmException {
        EffectiveKey key = new EffectiveKey("/orders", "POST", "user-1", "abc");

        MessageDigest expected = MessageDigest.getInstance("SHA-256");
        expected.update("/orders".getBytes(StandardCharsets.UTF_8));
        expected.update((byte) 0);
        expected.update("POST".getBytes(StandardCharsets.UTF_8));
        expected.update((byte) 0);
        expected.update("user-1".getBytes(StandardCharsets.UTF_8));
        expected.update((byte) 0);
        expected.update("abc".getBytes(StandardCharsets.UTF_8));

        assertThat(HexFormat.of().formatHex(key.digestBytes()))
                .isEqualTo(HexFormat.of().formatHex(expected.digest()));
    }
}
