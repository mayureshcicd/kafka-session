package com.example.failover;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** The same producer keeps working after one of the three replicated brokers fails. */
@RestController
public class FailoverMessages {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Message(String message) {}
    private final KafkaTemplate<String,String> kafka;
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public FailoverMessages(KafkaTemplate<String,String> kafka,DemoLog log) {
        this.kafka=kafka;
        this.log=log;
    }
    @PostMapping("/messages") @ResponseStatus(HttpStatus.ACCEPTED)
    // Wait for a safe Kafka write. The three-broker lab can continue with one broker stopped.
    public CompletableFuture<Map<String,String>> send(@RequestBody Message message) {
        if (message.message()==null || message.message().isBlank())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"message is required");
        String id=UUID.randomUUID().toString();
        // acks=all plus topic min ISR=2 rejects writes if too few safe copies remain.
        return kafka.send("sample.failover.messages",id,message.message()).thenApply(result -> Map.of("id",id));
    }
    @KafkaListener(topics="sample.failover.messages",groupId="sample-failover-reader")
    // Show that a published record was consumed, including during the one-broker failure test.
    public void read(ConsumerRecord<String,String> record) {
        log.add("received",record.key(),record.value());
    }
}
