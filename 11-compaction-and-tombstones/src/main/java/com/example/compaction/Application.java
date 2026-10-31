package com.example.compaction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Starts only this independent teaching project. */
@SpringBootApplication
public class Application {
    // Start this Spring Boot app. Spring then creates our controllers, listeners, and settings.
    public static void main(String[] args) {
        // Spring discovers this project's HTTP controllers, Kafka listeners, and settings.
        SpringApplication.run(Application.class, args);
    }
}
