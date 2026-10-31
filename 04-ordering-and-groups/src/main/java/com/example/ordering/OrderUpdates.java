package com.example.ordering;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** One business key keeps the updates for an order in the same Kafka partition. */
@RestController
public class OrderUpdates {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Update(String status) {}
    private final KafkaTemplate<String,String> kafka;
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public OrderUpdates(KafkaTemplate<String,String> kafka, DemoLog log) {
        this.kafka=kafka;
        this.log=log;
    }

    @PostMapping("/orders/{orderId}/events")
    @ResponseStatus(HttpStatus.ACCEPTED)
    // Use the order ID as the key so updates for that order use the same ordered lane.
    public CompletableFuture<Map<String,Object>> publish(@PathVariable String orderId, @RequestBody Update update) {
        if (update.status()==null || update.status().isBlank())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"status is required");
        // The order ID is the KEY, not only part of the value. The key chooses the lane.
        return kafka.send("sample.ordering.updates",orderId,update.status()).thenApply(result ->
                Map.of("orderId",orderId,"partition",result.getRecordMetadata().partition(),"offset",result.getRecordMetadata().offset()));
    }

    @KafkaListener(topics="sample.ordering.updates",groupId="sample-ordering-workers")
    // Show the status, lane number, and position so we can check the order of updates.
    public void handle(ConsumerRecord<String,String> record) {
        // Showing partition and offset makes the actual ordering visible.
        log.add("update",record.key(),record.value()+" partition="+record.partition()+" offset="+record.offset());
    }
}
