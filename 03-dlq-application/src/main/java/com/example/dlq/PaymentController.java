package com.example.dlq;


import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Accepts plain-text payment references. FAIL is our deliberate classroom failure. */
@RestController
public class PaymentController {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record PaymentRequest(String reference) {}
    private final KafkaTemplate<String, String> kafka;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public PaymentController(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    @PostMapping("/payments")
    @ResponseStatus(HttpStatus.ACCEPTED)
    // Publish the reference. FAIL is accepted on purpose so we can see downstream failure.
    public CompletableFuture<Map<String, String>> pay(@RequestBody PaymentRequest request) {
        if (request.reference() == null || request.reference().isBlank())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "reference is required");
        String id = UUID.randomUUID().toString();
        // We allow FAIL here on purpose so failure happens later in the Kafka listener.
        return kafka.send("sample.dlq.payments", id, request.reference())
                .thenApply(result -> Map.of("id", id, "reference", request.reference()));
    }
}
