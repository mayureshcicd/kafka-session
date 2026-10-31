package com.example.kafkademo;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
@RestController
@RequestMapping("/orders")
/** Small teaching component used to show Kafka message flow in this sample. */
public class OrderController {
    private final KafkaTemplate<String, OrderEvent> template;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public OrderController(KafkaTemplate<String, OrderEvent> template) {
        this.template = template;
    }
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    // Publish the order and wait for Kafka acknowledgement before returning HTTP 202.
    public CompletableFuture<OrderEvent> createOrder(@RequestBody CreateOrderRequest request) {
        if (request.customerId() == null || request.customerId().isBlank() || request.amount() == null)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "customerId and amount are required");
        // Amount 0 is intentionally accepted to demonstrate a downstream processing failure.
        var event = new OrderEvent(UUID.randomUUID().toString(), request.customerId(), request.amount());
        // Return 202 only after Kafka acknowledges the publish. It does not mean stock is reserved.
        return template.send("orders", event.orderId(), event).thenApply(result -> event);
    }
}
