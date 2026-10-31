package com.example.events;


import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Announces an order-created fact. Inventory and shipping are not called here. */
@RestController
public class OrderController {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record OrderRequest(String product, int quantity) {}
    private final KafkaTemplate<String, OrderEvent> kafka;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public OrderController(KafkaTemplate<String, OrderEvent> kafka) {
        this.kafka = kafka;
    }

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.ACCEPTED)
    // Publish the order-created fact. Other parts react later through Kafka, not direct calls.
    public CompletableFuture<OrderEvent> create(@RequestBody OrderRequest request) {
        if (request.product() == null || request.product().isBlank() || request.quantity() <= 0)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "product and positive quantity are required");
        var event = new OrderEvent(UUID.randomUUID().toString(), request.product(), request.quantity());
        // orderId is the key so this order's records use a consistent ordering lane.
        // HTTP 202 means Kafka accepted the fact, not that shipping has finished.
        return kafka.send("sample.events.orders-created", event.orderId(), event)
                .thenApply(result -> event);
    }
}
