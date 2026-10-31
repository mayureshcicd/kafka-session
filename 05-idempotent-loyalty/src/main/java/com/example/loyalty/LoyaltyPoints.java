package com.example.loyalty;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** The database, rather than an in-memory set, remembers which events were applied. */
@RestController
public class LoyaltyPoints {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Award(String eventId,String customer,int points) {}
    private final KafkaTemplate<String,String> kafka;
    private final JdbcTemplate db;
    private final ObjectMapper json;
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public LoyaltyPoints(KafkaTemplate<String,String> kafka,JdbcTemplate db,ObjectMapper json,DemoLog log) {
        this.kafka=kafka;
        this.db=db;
        this.json=json;
        this.log=log;
    }
    @PostMapping("/awards") @ResponseStatus(HttpStatus.ACCEPTED)
    // Accept one award event. The caller keeps the same event ID when retrying the same award.
    public CompletableFuture<Award> award(@RequestBody Award award) throws Exception {
        if (award.eventId()==null || award.eventId().isBlank() || award.customer()==null || award.customer().isBlank() || award.points()<=0)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"eventId, customer, and positive points are required");
        return kafka.send("sample.loyalty.awards",award.customer(),json.writeValueAsString(award)).thenApply(sent -> award);
    }
    @KafkaListener(topics="sample.loyalty.awards",groupId="sample-loyalty-workers")
    // Insert the award once. The database primary key stops a repeated event adding points twice.
    public void apply(String value) throws Exception {
        Award award=json.readValue(value,Award.class);
        try {
            // A single atomic INSERT records both the event ID and its points.
            // If a crash causes Kafka redelivery, the same primary key cannot be inserted twice.
            db.update("INSERT INTO awards(event_id,customer,points) VALUES (?,?,?)",award.eventId(),award.customer(),award.points());
            log.add("awarded",award.eventId(),award.customer()+" +"+award.points());
        } catch (DuplicateKeyException duplicate) {
            // Only the expected duplicate is ignored. Other DB failures must reach Kafka retry handling.
            log.add("duplicate",award.eventId(),"Already awarded; no points added");
        }
    }
    @GetMapping("/points/{customer}")
    // Add up the customer's saved award rows to show the durable points total.
    public Map<String,Object> points(@PathVariable String customer) {
        // Calculate from durable award rows; there is no separate balance to get out of sync.
        return Map.of("customer",customer,"points",db.queryForObject("SELECT COALESCE(SUM(points),0) FROM awards WHERE customer=?",Long.class,customer));
    }
}
