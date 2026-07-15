package io.adzubla.blocks.idempotency.store.redis;

import io.adzubla.blocks.idempotency.config.IdempotencyAutoConfiguration;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * Registers the Redis-backed store under the {@code "redis"} qualifier when Spring
 * Data Redis is on the classpath.
 *
 * <p>{@code @AutoConfigureBefore(IdempotencyAutoConfiguration.class)} is load-bearing:
 * that class's {@code EngineConfiguration} is gated on {@code @ConditionalOnBean(
 * IdempotencyStore.class)}, and without an explicit ordering the two auto-configurations
 * race on classpath/JAR scan order - if this one loses, the store bean doesn't exist yet
 * when the condition is evaluated, and the engine/interceptor silently never get
 * registered (every {@code @Idempotent} endpoint runs unprotected, with no error).
 *
 * <p>Also registers {@link RedisStoreProperties} ({@code idempotency.redis.key-prefix}).
 */
@AutoConfiguration
@AutoConfigureBefore(IdempotencyAutoConfiguration.class)
@ConditionalOnClass(StringRedisTemplate.class)
@EnableConfigurationProperties(RedisStoreProperties.class)
public class RedisIdempotencyStoreAutoConfiguration {

    @Bean
    @Qualifier(RedisIdempotencyStore.QUALIFIER)
    @ConditionalOnMissingBean(name = "redisIdempotencyStore")
    public IdempotencyStore redisIdempotencyStore(StringRedisTemplate redisTemplate, ObjectMapper objectMapper,
            RedisStoreProperties properties) {
        return new RedisIdempotencyStore(redisTemplate, objectMapper, properties.getKeyPrefix());
    }
}
