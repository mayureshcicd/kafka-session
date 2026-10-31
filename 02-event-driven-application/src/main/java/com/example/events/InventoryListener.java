package com.example.events;


import java.util.concurrent.TimeUnit;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Reacts to the first event and publishes a new fact for the next step. */
@Component
public class InventoryListener {
    private final KafkaTemplate<String, OrderEvent> kafka;
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public InventoryListener(KafkaTemplate<String, OrderEvent> kafka, DemoLog log) {
        this.kafka = kafka;
        this.log = log;
    }

    @KafkaListener(topics = "sample.events.orders-created", groupId = "sample-events-inventory")
    // React to the order, then publish a stock-reserved fact for the shipping step.
    public void reserve(OrderEvent event) throws Exception {
        // We simulate reservation. There is no stock database in this beginner sample.
        log.add("inventory", event.orderId(), "Reserved " + event.quantity() + " of " + event.product());
        // Shipping reacts to this new fact. We never call ShippingListener directly.
        // Wait for acknowledgement: a failed publish must fail this listener too.
        kafka.send("sample.events.stock-reserved", event.orderId(), event).get(10, TimeUnit.SECONDS);
        // A crash before the source commit can repeat this publish: this is at least once.
    }
}
