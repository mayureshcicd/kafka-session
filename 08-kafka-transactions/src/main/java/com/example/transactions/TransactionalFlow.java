package com.example.transactions;

import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Kafka commits both output records and the input offset together, or aborts them. */
@RestController
public class TransactionalFlow {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Input(String value) {}
    private final KafkaTemplate<String,String> kafka;
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public TransactionalFlow(KafkaTemplate<String,String> kafka,DemoLog log) {
        this.kafka=kafka;
        this.log=log;
    }
    @PostMapping("/process") @ResponseStatus(HttpStatus.ACCEPTED)
    // Put the starting message in Kafka using a small transaction for this HTTP request.
    public Map<String,String> process(@RequestBody Input input) {
        if (input.value()==null || input.value().isBlank())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"value is required");
        String id=UUID.randomUUID().toString();
        // The HTTP publisher is not inside a listener transaction, so start its own transaction.
        kafka.executeInTransaction(operations -> { operations.send("sample.tx.input",id,input.value()); return true; });
        return Map.of("id",id);
    }
    @KafkaListener(topics="sample.tx.input",groupId="sample-tx-processor")
    // Write both outputs in one Kafka transaction. An error causes both writes to be aborted.
    public void transform(ConsumerRecord<String,String> record) {
        // Boot starts a transaction before invoking this listener.
        kafka.send("sample.tx.first",record.key(),record.value());
        kafka.send("sample.tx.second",record.key(),record.value());
        if (record.value().equals("FAIL"))
            throw new IllegalStateException("Abort both outputs on purpose");
    }
    @KafkaListener(topics="sample.tx.first",groupId="sample-tx-first-reader")
    // Show only committed records from the first output topic.
    public void first(ConsumerRecord<String,String> record) {
        log.add("first",record.key(),record.value());
    }
    @KafkaListener(topics="sample.tx.second",groupId="sample-tx-second-reader")
    // Show only committed records from the second output topic.
    public void second(ConsumerRecord<String,String> record) {
        log.add("second",record.key(),record.value());
    }
    @Bean
    // After limited retries, record the lab's skip decision and save source progress safely.
    DefaultAfterRollbackProcessor<Object,Object> rollback(KafkaTemplate<Object,Object> template) {
        // After 3 failed attempts, intentionally skip this lab input and log the decision.
        // A production recoverer would normally publish failure details for investigation.
        var handler=new DefaultAfterRollbackProcessor<Object,Object>(
            (record,error) -> log.add("rolled-back",record.key().toString(),"Both outputs aborted; input skipped"),
            new FixedBackOff(1000L,2L),template,true);
        return handler;
    }
}
