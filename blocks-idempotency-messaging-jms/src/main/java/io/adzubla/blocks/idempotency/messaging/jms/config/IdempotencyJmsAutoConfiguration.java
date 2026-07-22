package io.adzubla.blocks.idempotency.messaging.jms.config;

import io.adzubla.blocks.idempotency.config.IdempotencyAutoConfiguration;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.messaging.jms.JmsIdempotencyAdvice;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.JmsListener;

/**
 * JMS messaging auto-configuration: registers the {@link JmsIdempotencyAdvice}
 * bean that intercepts every {@code @Idempotent} + {@code @JmsListener} method.
 *
 * <p>{@code @AutoConfigureAfter(IdempotencyAutoConfiguration.class)} paired
 * with {@code @ConditionalOnBean(IdempotencyEngineRegistry.class)} mirrors
 * {@code IdempotencyWebAutoConfiguration}'s exact technique (ADR 0006), also
 * used by the Kafka and RabbitMQ modules: without it, this class's condition
 * could be evaluated before core's {@code IdempotencyEngineRegistry} bean
 * exists, and the advice would silently never get registered.
 */
@AutoConfiguration
@ConditionalOnClass(JmsListener.class)
@AutoConfigureAfter(IdempotencyAutoConfiguration.class)
@ConditionalOnBean(IdempotencyEngineRegistry.class)
public class IdempotencyJmsAutoConfiguration {

    @Bean
    public JmsIdempotencyAdvice jmsIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties) {
        return new JmsIdempotencyAdvice(engineRegistry, properties);
    }
}
