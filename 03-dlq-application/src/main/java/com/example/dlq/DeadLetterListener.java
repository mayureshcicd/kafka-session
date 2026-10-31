package com.example.dlq;


import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Shows failed records and readable error details so somebody can investigate them. */
@Component
public class DeadLetterListener {
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public DeadLetterListener(DemoLog log) {
        this.log = log;
    }

    @KafkaListener(topics = "sample.dlq.payments.DLT", groupId = "sample-dlq-monitor")
    // Show the failed message and its reason. This does not fix or automatically replay it.
    public void inspect(ConsumerRecord<String, String> record) {
        // Spring adds this header when it publishes the original failed record to the DLT.
        var header = record.headers().lastHeader("kafka_dlt-exception-message");
        String reason = header == null ? "unknown" : new String(header.value(), StandardCharsets.UTF_8);
        log.add("DLQ", record.key(), "Original value=" + record.value() + "; reason=" + reason);
        // Reading does not delete, fix, or replay the record. Recovery is a separate decision.
    }
}
