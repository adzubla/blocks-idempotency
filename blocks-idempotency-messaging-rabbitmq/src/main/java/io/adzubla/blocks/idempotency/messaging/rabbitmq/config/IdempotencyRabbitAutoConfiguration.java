package io.adzubla.blocks.idempotency.messaging.rabbitmq.config;

import io.adzubla.blocks.idempotency.config.IdempotencyAutoConfiguration;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.messaging.rabbitmq.RabbitIdempotencyAdvice;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;

/**
 * RabbitMQ messaging auto-configuration: registers the {@link
 * RabbitIdempotencyAdvice} bean that intercepts every {@code @Idempotent} +
 * {@code @RabbitListener} method.
 *
 * <p>{@code @AutoConfigureAfter(IdempotencyAutoConfiguration.class)} paired
 * with {@code @ConditionalOnBean(IdempotencyEngineRegistry.class)} mirrors
 * {@code IdempotencyWebAutoConfiguration}'s exact technique (ADR 0006), also
 * used by the Kafka module's {@code IdempotencyKafkaAutoConfiguration}:
 * without it, this class's condition could be evaluated before core's
 * {@code IdempotencyEngineRegistry} bean exists, and the advice would
 * silently never get registered.
 */
@AutoConfiguration
@ConditionalOnClass(RabbitListener.class)
@AutoConfigureAfter(IdempotencyAutoConfiguration.class)
@ConditionalOnBean(IdempotencyEngineRegistry.class)
public class IdempotencyRabbitAutoConfiguration {

    @Bean
    public RabbitIdempotencyAdvice rabbitIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties) {
        return new RabbitIdempotencyAdvice(engineRegistry, properties);
    }
}
