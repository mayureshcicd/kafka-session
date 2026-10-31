package com.example.secure;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Both clients authenticate over TLS; Kafka ACLs allow alice and deny bob. */
@RestController
public class SecureOrders {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Message(String message) {}
    private final KafkaTemplate<String,String> alice,bob;
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public SecureOrders(@Qualifier("aliceTemplate") KafkaTemplate<String,String> alice,
            @Qualifier("bobTemplate") KafkaTemplate<String,String> bob,DemoLog log) {
        this.alice=alice;
        this.bob=bob;
        this.log=log;
    }
    @PostMapping("/publish/{user}") @ResponseStatus(HttpStatus.ACCEPTED)
    // Send with the chosen real TLS identity. Let Kafka ACLs decide whether the write is allowed.
    public Map<String,String> publish(@PathVariable String user,@RequestBody Message message) throws Exception {
        if (!user.equals("alice") && !user.equals("bob")) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Use alice or bob");
        if (message.message()==null || message.message().isBlank()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"message is required");
        String id=UUID.randomUUID().toString();
        try {
            // There is no Java permission rule for bob. The broker must reject the actual send.
            (user.equals("alice") ? alice : bob).send("sample.secure.orders",id,message.message()).get(20,TimeUnit.SECONDS);
            return Map.of("id",id,"identity",user);
        } catch (Exception error) {
            Throwable cause=error;
            while (cause!=null) {
                if (cause instanceof AuthorizationException)
                    throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Kafka ACL denied "+user,error);
                cause=cause.getCause();
            }
            throw error; // TLS or network failure is not misreported as an ACL denial.
        }
    }
    @KafkaListener(topics="sample.secure.orders",groupId="sample-secure-reader")
    // Show accepted messages using the lab admin reader.
    public void read(ConsumerRecord<String,String> record) {
        log.add("received",record.key(),record.value());
    }
}
