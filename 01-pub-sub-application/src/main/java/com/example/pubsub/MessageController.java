package com.example.pubsub;


import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Publishes once. It does not call either subscriber directly. */
@RestController
public class MessageController {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record MessageRequest(String message) {}
    private final KafkaTemplate<String, String> kafka;

    // KafkaTemplate is Spring's ready-to-use helper for publishing Kafka records.
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public MessageController(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    @PostMapping("/messages")
    @ResponseStatus(HttpStatus.ACCEPTED)
    // Check the request, publish one message, and reply only after Kafka confirms the write.
    public CompletableFuture<Map<String, String>> publish(@RequestBody MessageRequest request) {
        if (request.message() == null || request.message().isBlank())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "message is required");
        String id = UUID.randomUUID().toString();
        // The key identifies this message. The value is the text both groups will read.
        // Return 202 after Kafka confirms the write, not after subscribers finish.
        return kafka.send("sample.pubsub.messages", id, request.message())
                .thenApply(result -> Map.of("id", id, "message", request.message()));
    }
}
