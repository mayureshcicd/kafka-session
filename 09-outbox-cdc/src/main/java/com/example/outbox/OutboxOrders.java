package com.example.outbox;

import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Shows the database write and the independent CDC delivery as different steps. */
@RestController
public class OutboxOrders {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Order(String product,boolean fail) {}
    private final OrderStorage storage;
    private final JdbcTemplate db;
    private final ObjectMapper json;
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public OutboxOrders(OrderStorage storage,JdbcTemplate db,ObjectMapper json,DemoLog log) {
        this.storage=storage;
        this.db=db;
        this.json=json;
        this.log=log;
    }
    @PostMapping("/orders") @ResponseStatus(HttpStatus.CREATED)
    // Ask the database service to save the order. Debezium publishes the committed change later.
    public Map<String,String> order(@RequestBody Order order) throws Exception {
        if (order.product()==null || order.product().isBlank())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"product is required");
        return storage.save(order.product(),order.fail());
    }
    @GetMapping("/database-counts") public Map<String,Object> counts() {
        return Map.of("orders",db.queryForObject("SELECT COUNT(*) FROM orders",Long.class),"outbox",db.queryForObject("SELECT COUNT(*) FROM outbox",Long.class));
    }
    @KafkaListener(topics="sampleoutbox.public.outbox",groupId="sample-outbox-reader")
    // Read a database-change message from Debezium and show the outbox event inside it.
    public void cdc(@org.springframework.messaging.handler.annotation.Payload(required=false) String value) throws Exception {
        // Debezium can publish null tombstones on delete; they are not new outbox events.
        if (value==null) return;
        var envelope=json.readTree(value);
        var row=envelope.path("after");
        if (!row.isObject()) return;
        // We keep the raw CDC envelope visible rather than hiding it behind an event router.
        log.add("cdc-event",row.path("event_id").asText(),row.path("payload").asText());
    }
}
