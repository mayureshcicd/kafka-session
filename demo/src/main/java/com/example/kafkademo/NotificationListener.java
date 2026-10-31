package com.example.kafkademo;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
@Component
/** Small teaching component used to show Kafka message flow in this sample. */
public class NotificationListener {
    private final DemoJournal journal;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public NotificationListener(DemoJournal journal) {
        this.journal = journal;
    }
    @KafkaListener(topics = "orders", groupId = "notification-service")
    // React as an independent reader, even when the inventory reader has a problem.
    public void sendConfirmation(OrderEvent event) {
        journal.add("notification", event.orderId(), "order received notification (simulated)");
    }
}
