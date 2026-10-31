package com.example.schemas;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Registers schemas, publishes real Avro bytes, and reads using the stored writer schema. */
@RestController
public class AvroOrders {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Order(String orderId,int amount,int version,String note) {}
    private final RegistryClient registry;
    private final KafkaTemplate<String,byte[]> kafka;
    private final DemoLog log;
    private final ObjectMapper json;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public AvroOrders(RegistryClient registry,KafkaTemplate<String,byte[]> kafka,DemoLog log,ObjectMapper json) {
        this.registry=registry;
        this.kafka=kafka;
        this.log=log;
        this.json=json;
    }
    @PostMapping("/schemas/{version}")
    // Expose the Registry's real success or rejection response for the compatibility exercise.
    public ResponseEntity<String> register(@PathVariable String version) throws Exception {
        if (!java.util.Set.of("v1","v2","bad").contains(version))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Use v1, v2, or bad");
        var response=registry.register(version);
        // Forward the real Registry response, including 409 for an incompatible schema.
        return ResponseEntity.status(response.statusCode()).contentType(MediaType.APPLICATION_JSON).body(response.body());
    }
    @PostMapping("/orders") @ResponseStatus(HttpStatus.ACCEPTED)
    // Convert this order to Avro bytes and include its registered schema ID before publishing.
    public CompletableFuture<Map<String,Object>> order(@RequestBody Order order) throws Exception {
        if (order.orderId()==null || order.orderId().isBlank() || order.amount()<=0 || (order.version()!=1 && order.version()!=2))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"orderId, positive amount, and version 1 or 2 are required");
        String version="v"+order.version();
        var schema=registry.local(version);
        int schemaId=registry.registerId(version);
        var record=new GenericData.Record(schema);
        record.put("orderId",order.orderId()); record.put("amount",order.amount());
        if (order.version()==2) record.put("note",order.note());
        var bytes=new ByteArrayOutputStream();
        // Confluent-compatible framing: magic byte 0, 4-byte schema ID, then Avro binary data.
        bytes.write(0); bytes.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        var encoder=EncoderFactory.get().binaryEncoder(bytes,null);
        new GenericDatumWriter<GenericRecord>(schema).write(record,encoder); encoder.flush();
        return kafka.send("sample.schemas.orders",order.orderId(),bytes.toByteArray()).thenApply(sent -> Map.of("id",order.orderId(),"schemaId",schemaId));
    }
    @KafkaListener(topics="sample.schemas.orders",groupId="sample-schema-reader")
    // Use the schema ID to decode the Avro bytes back into understandable fields.
    public void read(ConsumerRecord<String,byte[]> record) throws Exception {
        byte[] bytes=record.value();
        if (bytes==null || bytes.length<5 || bytes[0]!=0) throw new IllegalArgumentException("Not framed Avro data");
        int schemaId=ByteBuffer.wrap(bytes,1,4).getInt();
        var schema=registry.lookup(schemaId);
        var decoder=DecoderFactory.get().binaryDecoder(bytes,5,bytes.length-5,null);
        var value=new GenericDatumReader<GenericRecord>(schema).read(null,decoder);
        log.add("decoded",record.key(),"schemaId="+schemaId+" "+value.toString());
    }
}
