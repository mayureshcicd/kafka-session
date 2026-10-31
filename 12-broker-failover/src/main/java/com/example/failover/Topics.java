package com.example.failover;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;

/** Explicit topics make the partition and replica choices visible to learners. */
@Configuration
public class Topics {
    @Bean NewTopic topic0() {
        // Partitions split work; replicas copy data to other brokers.
        return TopicBuilder.name("sample.failover.messages").partitions(3).replicas(3).config("min.insync.replicas","2").build();
    }
}
