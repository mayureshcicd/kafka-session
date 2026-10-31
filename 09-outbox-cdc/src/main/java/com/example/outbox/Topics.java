package com.example.outbox;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;
/** Creates the topic before listeners start; one replica is for the local lab only. */
@Configuration
public class Topics {
    @Bean NewTopic topic() {
        return TopicBuilder.name("sampleoutbox.public.outbox").partitions(1).replicas(1).build();
    }
}
