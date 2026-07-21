package io.adzubla.blocks.idempotency.messaging.kafka.config;

import io.adzubla.blocks.idempotency.config.IdempotencyAutoConfiguration;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.messaging.kafka.KafkaIdempotencyAdvice;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Kafka messaging auto-configuration: registers the {@link
 * KafkaIdempotencyAdvice} bean that intercepts every {@code @Idempotent} +
 * {@code @KafkaListener} method.
 *
 * <p>{@code @AutoConfigureAfter(IdempotencyAutoConfiguration.class)} paired
 * with {@code @ConditionalOnBean(IdempotencyEngineRegistry.class)} mirrors
 * {@code IdempotencyWebAutoConfiguration}'s exact technique (ADR 0006):
 * without it, this class's condition could be evaluated before core's
 * {@code IdempotencyEngineRegistry} bean exists, and the advice would
 * silently never get registered.
 */
@AutoConfiguration
@ConditionalOnClass(KafkaListener.class)
@AutoConfigureAfter(IdempotencyAutoConfiguration.class)
@ConditionalOnBean(IdempotencyEngineRegistry.class)
public class IdempotencyKafkaAutoConfiguration {

    @Bean
    public KafkaIdempotencyAdvice kafkaIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties) {
        return new KafkaIdempotencyAdvice(engineRegistry, properties);
    }
}
