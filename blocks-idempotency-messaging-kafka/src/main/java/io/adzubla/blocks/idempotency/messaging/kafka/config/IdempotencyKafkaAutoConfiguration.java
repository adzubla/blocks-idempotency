package io.adzubla.blocks.idempotency.messaging.kafka.config;

import io.adzubla.blocks.idempotency.config.IdempotencyAutoConfiguration;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.messaging.kafka.KafkaDeadLetterPublisher;
import io.adzubla.blocks.idempotency.messaging.kafka.KafkaIdempotencyAdvice;
import io.adzubla.blocks.idempotency.messaging.kafka.validation.KafkaIdempotentListenerValidator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Kafka messaging auto-configuration: registers the {@link
 * KafkaIdempotencyAdvice} bean that intercepts every {@code @Idempotent} +
 * {@code @KafkaListener} method, plus the {@link KafkaDeadLetterPublisher} it
 * uses for terminal deliveries (currently collision, Slice 036).
 *
 * <p>{@code @AutoConfigureAfter(IdempotencyAutoConfiguration.class)} paired
 * with {@code @ConditionalOnBean(IdempotencyEngineRegistry.class)} mirrors
 * {@code IdempotencyWebAutoConfiguration}'s exact technique (ADR 0006):
 * without it, this class's condition could be evaluated before core's
 * {@code IdempotencyEngineRegistry} bean exists, and the advice would
 * silently never get registered.
 *
 * <p>The {@code KafkaTemplate} used for dead-lettering is resolved
 * optionally: an application with no producer configured simply can't
 * dead-letter, which only surfaces as a thrown exception the moment a
 * collision actually occurs, rather than blocking every {@code @Idempotent}
 * listener from being registered at all.
 *
 * <p>Also registers the {@link KafkaIdempotentListenerValidator} (Slice 040)
 * that fails startup fast on a misconfigured {@code @Idempotent}
 * {@code @KafkaListener} method, including {@code whenInProgress=WAIT}
 * (ADR 0005) and one whose store qualifier resolves to no bean at all.
 */
@AutoConfiguration
@ConditionalOnClass(KafkaListener.class)
@AutoConfigureAfter(IdempotencyAutoConfiguration.class)
@ConditionalOnBean(IdempotencyEngineRegistry.class)
@EnableConfigurationProperties(IdempotencyKafkaProperties.class)
public class IdempotencyKafkaAutoConfiguration {

    @SuppressWarnings("rawtypes")
    @Bean
    public KafkaDeadLetterPublisher kafkaDeadLetterPublisher(ObjectProvider<KafkaTemplate> kafkaTemplateProvider,
            IdempotencyKafkaProperties kafkaProperties) {
        return new KafkaDeadLetterPublisher(kafkaTemplateProvider.getIfAvailable(), kafkaProperties.getDeadLetterSuffix());
    }

    @Bean
    public KafkaIdempotencyAdvice kafkaIdempotencyAdvice(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties,
            KafkaDeadLetterPublisher deadLetterPublisher) {
        return new KafkaIdempotencyAdvice(engineRegistry, properties, deadLetterPublisher);
    }

    @Bean
    public KafkaIdempotentListenerValidator kafkaIdempotentListenerValidator(ConfigurableListableBeanFactory beanFactory,
            IdempotencyProperties properties) {
        return new KafkaIdempotentListenerValidator(beanFactory, properties);
    }
}
