package com.example.events;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Reacts only after the stock-reserved event exists. */
@Component
public class ShippingListener {
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public ShippingListener(DemoLog log) {
        this.log = log;
    }

    @KafkaListener(topics = "sample.events.stock-reserved", groupId = "sample-events-shipping")
    // React only after a stock-reserved event arrives. This lab logs the shipping action.
    public void ship(OrderEvent event) {
        // Logging stands in for a real shipping system so the project needs no other service.
        log.add("shipping", event.orderId(), "Shipment prepared for " + event.product());
    }
}
