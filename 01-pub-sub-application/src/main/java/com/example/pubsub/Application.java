package com.example.pubsub;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Starts this sample only. The other two samples are independent applications. */
@SpringBootApplication
public class Application {
    // Start this Spring Boot app. Spring then creates our controllers, listeners, and settings.
    public static void main(String[] args) {
        // Spring finds our controllers, listeners, and Kafka settings in this package.
        SpringApplication.run(Application.class, args);
    }
}
