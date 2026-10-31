package com.example.kafkademo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;
@Configuration
/** Small teaching component used to show Kafka message flow in this sample. */
public class KafkaConfiguration {
    @Value("${demo.replication-factor:1}") private int replicas;
    // Make the topic settings visible: lanes split work and replicas store copies.
    private NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(3).replicas(replicas).build();
    }
    @Bean NewTopic ordersTopic() {
        return topic("orders");
    }
    @Bean NewTopic ordersDeadLetterTopic() {
        return topic("orders.DLT");
    }
    @Bean NewTopic reservedTopic() {
        return topic("inventory.reserved");
    }
    @Bean
    // Give only the inventory reader a limited retry policy and a dead-letter destination.
    ConcurrentKafkaListenerContainerFactory<Object, Object> inventoryFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory, KafkaTemplate<Object, Object> template) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition("orders.DLT", record.partition()));
        // If DLT publish fails, throw: do not mark the source record as recovered.
        recoverer.setFailIfSendResultIsError(true);
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        factory.setCommonErrorHandler(handler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
