package com.example.kafkademo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
@Component
/** Small teaching component used to show Kafka message flow in this sample. */
public class DeadLetterListener {
    private final DemoJournal journal;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public DeadLetterListener(DemoJournal journal) {
        this.journal = journal;
    }
    // Turn header bytes into readable text so the classroom log is easy to understand.
    private String header(ConsumerRecord<String, OrderEvent> record, String name) {
        var value = record.headers().lastHeader(name);
        return value == null ? "unknown" : new String(value.value(), java.nio.charset.StandardCharsets.UTF_8);
    }
    @KafkaListener(topics = "orders.DLT", groupId = "dead-letter-monitor")
    // Show the failed order and source information so someone can investigate it.
    public void inspectFailedOrder(ConsumerRecord<String, OrderEvent> record) {
        journal.add("DLQ", record.value().orderId(), "orders.DLT partition=" + record.partition()
                + " offset=" + record.offset() + " source=" + header(record, "kafka_dlt-original-topic")
                + " error=" + header(record, "kafka_dlt-exception-message"));
        // Reading a dead-letter topic does not delete the record, fix it, or replay it.
    }
}
