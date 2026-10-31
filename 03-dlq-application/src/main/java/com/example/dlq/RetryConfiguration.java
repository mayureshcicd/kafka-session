package com.example.dlq;


import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/** Kafka does not move failed Java calls to a DLQ automatically. We configure that here. */
@Configuration
public class RetryConfiguration {
    @Bean
    // Build the payment listener with two retries and a dead-letter topic for final failure.
    ConcurrentKafkaListenerContainerFactory<Object, Object> paymentFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<Object, Object> kafka) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        // Reuse Spring Boot's serializers, broker address, and consumer properties.
        configurer.configure(factory, consumerFactory);

        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                // Keep the original partition number. Both topics therefore have 3 partitions.
                (record, error) -> new TopicPartition("sample.dlq.payments.DLT", record.partition()));
        // A failed DLQ write must not silently count as successful recovery.
        recoverer.setFailIfSendResultIsError(true);

        // 1000 milliseconds between retries, 2 retries AFTER the first call = 3 attempts.
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        factory.setCommonErrorHandler(handler);
        // Commit source progress after success or successful DLQ recovery.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
