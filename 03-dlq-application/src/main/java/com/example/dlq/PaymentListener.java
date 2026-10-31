package com.example.dlq;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Demonstrates a processing error after Kafka successfully stored a record. */
@Component
public class PaymentListener {
    private final DemoLog log;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public PaymentListener(DemoLog log) {
        this.log = log;
    }

    // Only this listener uses our retry/DLQ factory. The DLQ monitor has a different job.
    @KafkaListener(topics = "sample.dlq.payments", groupId = "sample-dlq-payment",
            containerFactory = "paymentFactory")
    // Try the business work. Throwing an error tells Spring that this attempt failed.
    public void process(ConsumerRecord<String, String> record) {
        log.add("attempt", record.key(), record.value());
        if ("FAIL".equalsIgnoreCase(record.value())) {
            // Throwing lets Spring's error handler retry; catching and ignoring would skip recovery.
            throw new IllegalStateException("Demo payment failed on purpose");
        }
        // No money is moved. This log represents successful business work.
        log.add("payment-success", record.key(), record.value());
    }
}
