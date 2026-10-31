package com.example.kafkademo;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
@Component
/** Small teaching component used to show Kafka message flow in this sample. */
public class InventoryListener {
    private final DemoJournal journal;
    private final KafkaTemplate<String, OrderEvent> template;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public InventoryListener(DemoJournal journal, KafkaTemplate<String, OrderEvent> template) {
        this.journal = journal;
        this.template = template;
    }
    @KafkaListener(topics = "orders", groupId = "inventory-service", containerFactory = "inventoryFactory")
    // Do the inventory step and let an error reach Spring so retries can happen.
    public void reserveStock(OrderEvent event) throws Exception {
        journal.add("inventory-attempt", event.orderId(), "trying to reserve stock");
        if (event.amount() == null || event.amount().signum() <= 0)
            throw new IllegalStateException("DEMO: amount must be greater than zero");
        // A second event makes the event-driven chain visible. No real stock database in this demo.
        template.send("inventory.reserved", event.orderId(), event).get(10, java.util.concurrent.TimeUnit.SECONDS);
        journal.add("inventory-success", event.orderId(), "published inventory.reserved");
    }
}
