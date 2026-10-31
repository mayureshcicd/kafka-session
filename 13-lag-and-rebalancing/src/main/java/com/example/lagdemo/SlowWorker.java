package com.example.lagdemo;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** A controlled slow consumer lets us observe real lag and group rebalancing. */
@RestController
public class SlowWorker {
    private final KafkaTemplate<String,String> kafka;
    private final KafkaProperties settings;
    private final DemoLog log;
    @Value("${demo.delay-ms}") private long delay;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public SlowWorker(KafkaTemplate<String,String> kafka,KafkaProperties settings,DemoLog log) {
        this.kafka=kafka;
        this.settings=settings;
        this.log=log;
    }
    @PostMapping("/work") @ResponseStatus(HttpStatus.ACCEPTED)
    // Publish a small burst across three lanes so lag is visible in the demonstration.
    public CompletableFuture<Map<String,Object>> work(@RequestParam(defaultValue="30") int count) {
        if (count<1 || count>100) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"count must be 1..100");
        var sends=new ArrayList<CompletableFuture<?>>();
        String batch=UUID.randomUUID().toString();
        for (int i=0;i<count;i++) {
            // Round-robin explicit partitions make all three lanes busy for this controlled lab.
            var record=new org.apache.kafka.clients.producer.ProducerRecord<String,String>("sample.lag.work",i%3,batch+"-"+i,"work");
            sends.add(kafka.send(record));
        }
        return CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).thenApply(done -> Map.of("batch",batch,"count",count));
    }
    @KafkaListener(topics="sample.lag.work",groupId="sample-slow-workers")
    // Delay each record on purpose to imitate slow business work, then show completion.
    public void process(ConsumerRecord<String,String> record) throws InterruptedException {
        // Deliberately slow the business work. This is not a recommended retry technique.
        Thread.sleep(delay);
        log.add("processed",record.key(),"partition="+record.partition()+" delayMs="+delay);
    }
    @GetMapping("/lag")
    // Ask Kafka for saved positions and log ends; subtract them to show actual group lag.
    public List<Map<String,Object>> lag() throws Exception {
        try (AdminClient admin=AdminClient.create(settings.buildAdminProperties())) {
            var committed=admin.listConsumerGroupOffsets("sample-slow-workers").partitionsToOffsetAndMetadata().get();
            var request=new HashMap<TopicPartition,OffsetSpec>();
            for (int p=0;p<3;p++) request.put(new TopicPartition("sample.lag.work",p),OffsetSpec.latest());
            var ends=admin.listOffsets(request).all().get();
            var rows=new ArrayList<Map<String,Object>>();
            for (var item:ends.entrySet()) {
                var bookmark=committed.get(item.getKey());
                // Before the first commit, a new group's earliest position is 0 in this lab.
                long offset=bookmark==null ? 0 : bookmark.offset();
                rows.add(Map.of("partition",item.getKey().partition(),"committed",offset,"end",item.getValue().offset(),"lag",item.getValue().offset()-offset));
            }
            return rows;
        }
    }
}
