package com.example.kafkademo;

import java.math.BigDecimal;

// A record is a small data holder. Its fields describe the request or event we send.
/** Small teaching component used to show Kafka message flow in this sample. */
public record CreateOrderRequest(String customerId, BigDecimal amount) {
}
