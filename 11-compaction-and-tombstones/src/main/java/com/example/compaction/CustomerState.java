package com.example.compaction;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** A keyed update replaces latest state; a null value is a deletion tombstone. */
@RestController
public class CustomerState {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Name(String name) {}
    private final KafkaTemplate<String,String> kafka;
    private final KafkaProperties settings;
    private final Map<String,String> current=new ConcurrentHashMap<>();
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public CustomerState(KafkaTemplate<String,String> kafka,KafkaProperties settings) {
        this.kafka=kafka;
        this.settings=settings;
    }
    @PostMapping("/customers/{id}") @ResponseStatus(HttpStatus.ACCEPTED)
    // Publish the new name using the customer ID as the key.
    public CompletableFuture<Map<String,String>> update(@PathVariable String id,@RequestBody Name name) {
        if (name.name()==null || name.name().isBlank())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"name is required");
        return kafka.send("sample.compaction.customers",id,name.name()).thenApply(result -> Map.of("id",id));
    }
    @DeleteMapping("/customers/{id}") @ResponseStatus(HttpStatus.ACCEPTED)
    // Publish a real null value to mark this customer as deleted.
    public CompletableFuture<Map<String,String>> delete(@PathVariable String id) {
        // A real null value is a tombstone. The string "null" would be a normal value.
        return kafka.send("sample.compaction.customers",id,(String)null).thenApply(result -> Map.of("id",id));
    }
    // A fresh group intentionally replays available state whenever this app restarts.
    @KafkaListener(topics="sample.compaction.customers",groupId="sample-compaction-${random.uuid}")
    // Update the latest-state view, or remove the customer when a deletion marker arrives.
    public void apply(ConsumerRecord<String,String> record) {
        if (record.value()==null) current.remove(record.key()); else current.put(record.key(),record.value());
    }
    @GetMapping("/customers") public Map<String,String> customers() {
        return Map.copyOf(current);
    }
    @GetMapping("/rebuild") public Map<String,String> rebuild() {
        var recovered=new HashMap<String,String>();
        // A separate manual-assignment consumer cannot disturb the live listener's bookmarks.
        var config=settings.buildConsumerProperties();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        var partition=new TopicPartition("sample.compaction.customers",0);
        try (var reader=new KafkaConsumer<String,String>(config)) {
            reader.assign(List.of(partition)); reader.seekToBeginning(List.of(partition));
            long end=reader.endOffsets(List.of(partition)).get(partition);
            long deadline=System.currentTimeMillis()+15000;
            while (reader.position(partition)<end) {
                if (System.currentTimeMillis()>deadline)
                    throw new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT,"Rebuild took too long");
                for (var record:reader.poll(Duration.ofMillis(500))) {
                    if (record.offset()>=end) continue; // Rebuild only the captured history.
                    if (record.value()==null) recovered.remove(record.key()); else recovered.put(record.key(),record.value());
                }
            }
        }
        return recovered;
    }
}
