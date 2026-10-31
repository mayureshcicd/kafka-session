package com.example.transactions;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;

/** Explicit topics make the partition and replica choices visible to learners. */
@Configuration
public class Topics {
    @Bean NewTopic topic0() {
        // Partitions split work; replicas copy data to other brokers.
        return TopicBuilder.name("sample.tx.input").partitions(3).replicas(1).build();
    }
    @Bean NewTopic topic1() {
        // Partitions split work; replicas copy data to other brokers.
        return TopicBuilder.name("sample.tx.first").partitions(3).replicas(1).build();
    }
    @Bean NewTopic topic2() {
        // Partitions split work; replicas copy data to other brokers.
        return TopicBuilder.name("sample.tx.second").partitions(3).replicas(1).build();
    }
}
