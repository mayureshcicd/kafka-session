package com.example.pubsub;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** One independent subscriber. The business action is simulated by a log entry. */
@Component
public class AuditSubscriber {
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public AuditSubscriber(DemoLog log) {
        this.log = log;
    }

    // Different group IDs mean email AND audit both read the same published record.
    // A second app with this SAME group ID would share its partitions instead.
    @KafkaListener(topics = "sample.pubsub.messages", groupId = "sample-pubsub-audit")
    // Read the same message independently as the audit group. Reading does not delete it.
    public void receive(ConsumerRecord<String, String> record) {
        log.add("audit", record.key(), record.value());
    }
}
