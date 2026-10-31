package com.example.retrydemo;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Retry topics allow a later FAST email to finish before an earlier RETRY email. */
@RestController
public class EmailRetries {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Email(String message) {}
    private final KafkaTemplate<String,String> kafka;
    private final DemoLog log;
    // This is only a simulated flaky provider. Attempt counts reset on app restart.
    private final Map<String,Integer> attempts=new ConcurrentHashMap<>();
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public EmailRetries(KafkaTemplate<String,String> kafka,DemoLog log) {
        this.kafka=kafka;
        this.log=log;
    }
    @PostMapping("/emails") @ResponseStatus(HttpStatus.ACCEPTED)
    // Publish an email request without waiting for the email provider to finish.
    public CompletableFuture<Map<String,String>> send(@RequestBody Email email) {
        if (email.message()==null || email.message().isBlank())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"message is required");
        String id=UUID.randomUUID().toString();
        return kafka.send("sample.retry.emails",id,email.message()).thenApply(result -> Map.of("id",id));
    }
    // Spring creates the retry/DLT topics and listeners. A retry is not a Thread.sleep here.
    @RetryableTopic(attempts="3",backoff=@Backoff(delay=3000),numPartitions="1",replicationFactor="1")
    @KafkaListener(topics="sample.retry.emails",groupId="sample-retry-email")
    // Try the email step. Spring sends failures to retry topics so later work can continue.
    public void receive(ConsumerRecord<String,String> record) {
        int attempt=attempts.merge(record.key(),1,Integer::sum);
        log.add("attempt",record.key(),"attempt="+attempt+" topic="+record.topic());
        if (record.value().equals("FAIL") || (record.value().equals("RETRY") && attempt<3))
            throw new IllegalStateException("Simulated temporary email failure");
        log.add("email-sent",record.key(),record.value());
        attempts.remove(record.key());
    }
    @DltHandler
    // Show a message that still failed after all allowed attempts.
    public void failed(ConsumerRecord<String,String> record) {
        log.add("DLQ",record.key(),record.value());
        attempts.remove(record.key());
    }
}
