package com.example.outbox;

import java.util.Map;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** This separate bean ensures Spring applies a real DATABASE transaction around both INSERTs. */
@Service
public class OrderStorage {
    private final JdbcTemplate db;
    private final ObjectMapper json;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public OrderStorage(JdbcTemplate db,ObjectMapper json) {
        this.db=db;
        this.json=json;
    }
    @Transactional
    // Save the order and outbox row in one database transaction so both stay or both roll back.
    public Map<String,String> save(String product,boolean fail) throws Exception {
        String id=UUID.randomUUID().toString(),eventId=UUID.randomUUID().toString();
        db.update("INSERT INTO orders(id,product) VALUES (?,?)",id,product);
        db.update("INSERT INTO outbox(event_id,order_id,payload) VALUES (?,?,?)",eventId,id,json.writeValueAsString(Map.of("orderId",id,"product",product)));
        // A deliberate failure proves both database INSERTs roll back together.
        if (fail) throw new IllegalStateException("Demo rollback after both INSERTs");
        // There is no Kafka send here: Debezium publishes committed WAL changes later.
        return Map.of("orderId",id,"eventId",eventId);
    }
}
