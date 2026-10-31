package com.example.dashboard;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Counts orders per customer in one-minute EVENT-TIME windows, using a real state store. */
@RestController
@EnableKafkaStreams
public class MinuteDashboard {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Order(String customer,Long eventTimeMs) {}
    private final KafkaTemplate<String,String> kafka;
    private final StreamsBuilderFactoryBean streams;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public MinuteDashboard(KafkaTemplate<String,String> kafka,StreamsBuilderFactoryBean streams) {
        this.kafka=kafka;
        this.streams=streams;
    }
    @Bean
    // Describe the stream: group by customer, count per minute, and save the running counts.
    KStream<String,String> topology(StreamsBuilder builder) {
        KStream<String,String> orders=builder.stream("sample.streams.orders",Consumed.with(Serdes.String(),Serdes.String()));
        // A ten-second grace allows late records until stream time closes their window.
        orders.groupByKey().windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1),Duration.ofSeconds(10)))
            .count(Materialized.<String,Long,WindowStore<org.apache.kafka.common.utils.Bytes,byte[]>>as("minute-counts")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()).withRetention(Duration.ofDays(1)));
        // Kafka Streams maintains the store and its changelog topic for recovery.
        return orders;
    }
    @PostMapping("/orders") @ResponseStatus(HttpStatus.ACCEPTED)
    // Publish one order with an event time so we can also demonstrate late arrival.
    public CompletableFuture<Map<String,Object>> order(@RequestBody Order order) {
        if (order.customer()==null || order.customer().isBlank())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"customer is required");
        long time=order.eventTimeMs()==null ? System.currentTimeMillis() : order.eventTimeMs();
        if (time<0) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"eventTimeMs must not be negative");
        // Setting the Kafka timestamp lets us demonstrate late events deliberately.
        return kafka.send(new ProducerRecord<>("sample.streams.orders",null,time,order.customer(),"1"))
            .thenApply(result -> Map.of("customer",order.customer(),"eventTimeMs",time));
    }
    @GetMapping("/latest-window")
    // Show the newest saved minute so a repeated time-based exercise can move forward safely.
    public Map<String,Long> latestWindow() {
        // Expose the newest retained window so a repeated late-event exercise can advance time safely.
        KafkaStreams running=streams.getKafkaStreams();
        if (running==null || running.state()!=KafkaStreams.State.RUNNING)
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Streams is still starting");
        ReadOnlyWindowStore<String,Long> store=running.store(StoreQueryParameters.fromNameAndType("minute-counts",QueryableStoreTypes.windowStore()));
        long latest=0;
        try (var iterator=store.all()) {
            while (iterator.hasNext()) latest=Math.max(latest,iterator.next().key.window().start());
        }
        return Map.of("windowStartMs",latest);
    }
    @GetMapping("/counts/{customer}")
    // Read this customer's minute totals from the real Kafka Streams store.
    public List<Map<String,Object>> counts(@PathVariable String customer) {
        KafkaStreams running=streams.getKafkaStreams();
        if (running==null || running.state()!=KafkaStreams.State.RUNNING)
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Streams is still starting");
        ReadOnlyWindowStore<String,Long> store=running.store(StoreQueryParameters.fromNameAndType("minute-counts",QueryableStoreTypes.windowStore()));
        var rows=new ArrayList<Map<String,Object>>();
        // Read the actual materialized store, not an imitation in a Java map.
        try (WindowStoreIterator<Long> iterator=store.fetch(customer,Instant.EPOCH,Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (iterator.hasNext()) { var row=iterator.next(); rows.add(Map.of("windowStartMs",row.key,"count",row.value)); }
        }
        return rows;
    }
}
