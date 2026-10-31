package com.example.compaction;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;

/** Explicit topics make the partition and replica choices visible to learners. */
@Configuration
public class Topics {
    @Bean NewTopic topic0() {
        // Partitions split work; replicas copy data to other brokers.
        return TopicBuilder.name("sample.compaction.customers").partitions(1).replicas(1).config("cleanup.policy","compact").config("delete.retention.ms","86400000").build();
    }
}
