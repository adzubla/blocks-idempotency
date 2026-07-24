package io.adzubla.blocks.idempotency.messaging.jms.config;

import io.adzubla.blocks.idempotency.config.IdempotencyAutoConfiguration;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.messaging.jms.JmsDeadLetterPublisher;
import io.adzubla.blocks.idempotency.messaging.jms.JmsIdempotencyAdvice;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;

/**
 * JMS messaging auto-configuration: registers the {@link JmsIdempotencyAdvice}
 * bean that intercepts every {@code @Idempotent} + {@code @JmsListener}
 * method, plus the {@link JmsDeadLetterPublisher} it uses for terminal
 * deliveries (collision, missing/invalid key - Slice 050).
 *
 * <p>{@code @AutoConfigureAfter(IdempotencyAutoConfiguration.class)} paired
 * with {@code @ConditionalOnBean(IdempotencyEngineRegistry.class)} mirrors
 * {@code IdempotencyWebAutoConfiguration}'s exact technique (ADR 0006), also
 * used by the Kafka and RabbitMQ modules: without it, this class's condition
 * could be evaluated before core's {@code IdempotencyEngineRegistry} bean
 * exists, and the advice would silently never get registered.
 *
 * <p>The {@code JmsTemplate} used for dead-lettering is resolved optionally,
 * the same technique {@code IdempotencyKafkaAutoConfiguration} uses for its
 * {@code KafkaTemplate}: an application with no producer configured simply
 * can't dead-letter, which only surfaces as a thrown exception the moment a
 * collision actually occurs, rather than blocking every {@code @Idempotent}
 * listener from being registered at all.
 */
@AutoConfiguration
@ConditionalOnClass(JmsListener.class)
@AutoConfigureAfter(IdempotencyAutoConfiguration.class)
@ConditionalOnBean(IdempotencyEngineRegistry.class)
@EnableConfigurationProperties(IdempotencyJmsProperties.class)
public class IdempotencyJmsAutoConfiguration {

    @Bean
    public JmsDeadLetterPublisher jmsDeadLetterPublisher(ObjectProvider<JmsTemplate> jmsTemplateProvider,
            IdempotencyJmsProperties jmsProperties) {
        return new JmsDeadLetterPublisher(jmsTemplateProvider.getIfAvailable(), jmsProperties.getDeadLetterSuffix());
    }

    @Bean
    public JmsIdempotencyAdvice jmsIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties,
            JmsDeadLetterPublisher deadLetterPublisher) {
        return new JmsIdempotencyAdvice(engineRegistry, properties, deadLetterPublisher);
    }
}
