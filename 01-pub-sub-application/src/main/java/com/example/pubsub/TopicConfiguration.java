package com.example.pubsub;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Creates only this sample's topics; names do not overlap with other projects. */
@Configuration
public class TopicConfiguration {
    @Bean
    NewTopic topic1() {
        // Three lanes let us later demonstrate consumer-group work sharing.
        // One replica is enough for this local lab; it provides no broker failover.
        return TopicBuilder.name("sample.pubsub.messages").partitions(3).replicas(1).build();
    }

}
