package io.adzubla.blocks.idempotency.store.redis;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Redis-store-specific settings (Slice 015).
 */
@ConfigurationProperties(prefix = "idempotency.redis")
public class RedisStoreProperties {

    /** Prefix prepended to the hashed record key (ADR 0001's Redis key structure). */
    private String keyPrefix = "idempotency:";

    public String getKeyPrefix() { return keyPrefix; }
    public void setKeyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; }
}
