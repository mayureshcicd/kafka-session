package com.example.pubsub;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** One independent subscriber. The business action is simulated by a log entry. */
@Component
public class EmailSubscriber {
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public EmailSubscriber(DemoLog log) {
        this.log = log;
    }

    // Different group IDs mean email AND audit both read the same published record.
    // A second app with this SAME group ID would share its partitions instead.
    @KafkaListener(topics = "sample.pubsub.messages", groupId = "sample-pubsub-email")
    // Read the published message as the email group. The audit group has its own progress.
    public void receive(ConsumerRecord<String, String> record) {
        log.add("email", record.key(), record.value());
    }
}
