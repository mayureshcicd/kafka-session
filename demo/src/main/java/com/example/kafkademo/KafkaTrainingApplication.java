package com.example.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
/** Small teaching component used to show Kafka message flow in this sample. */
public class KafkaTrainingApplication {
    // Start this Spring Boot app. Spring then creates our controllers, listeners, and settings.
    public static void main(String[] args) {
        SpringApplication.run(KafkaTrainingApplication.class, args);
    }
}
