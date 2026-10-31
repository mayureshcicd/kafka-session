package com.example.kafkademo;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
@Component
/** Small teaching component used to show Kafka message flow in this sample. */
public class ShippingListener {
    private final DemoJournal journal;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public ShippingListener(DemoJournal journal) {
        this.journal = journal;
    }
    @KafkaListener(topics = "inventory.reserved", groupId = "shipping-service")
    // Show that a second Kafka event, rather than a direct method call, triggers shipping.
    public void prepareShipment(OrderEvent event) {
        journal.add("shipping", event.orderId(), "shipment prepared (simulated)");
    }
}
