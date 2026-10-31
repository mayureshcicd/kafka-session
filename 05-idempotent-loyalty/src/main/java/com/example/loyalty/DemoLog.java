package com.example.loyalty;


import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import org.springframework.stereotype.Component;

/** A small classroom log so we can see results through an HTTP endpoint. */
@Component
public class DemoLog {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Entry(String time, String step, String id, String message) {}
    private final ArrayDeque<Entry> entries = new ArrayDeque<>();

    // Different Kafka listeners use different threads. synchronized keeps the list safe.
    // Save one result safely because different Kafka listeners can call this method at the same time.
    public synchronized void add(String step, String id, String message) {
        // Keep only 200 entries so a long-running demo does not use unlimited memory.
        if (entries.size() == 200) entries.removeFirst();
        entries.addLast(new Entry(Instant.now().toString(), step, id, message));
        System.out.printf("[%s] id=%s %s%n", step, id, message);
    }

    // Return a copy so a screen or HTTP request cannot change our saved results.
    public synchronized List<Entry> entries() {
        // Return a copy so an HTTP reader cannot change the stored entries.
        return List.copyOf(entries);
    }
}
