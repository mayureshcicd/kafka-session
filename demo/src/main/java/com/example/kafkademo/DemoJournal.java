package com.example.kafkademo;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import org.springframework.stereotype.Component;
@Component
/** Small teaching component used to show Kafka message flow in this sample. */
public class DemoJournal {
    // A record is a small data holder. Its fields describe the request or event we send.
    public record Entry(String time, String stage, String orderId, String detail) {}
    private final ArrayDeque<Entry> entries = new ArrayDeque<>();
    // Save a small, thread-safe teaching history and print the same step to the terminal.
    public synchronized void add(String stage, String id, String detail) {
        if (entries.size() == 200) entries.removeFirst();
        entries.addLast(new Entry(Instant.now().toString(), stage, id, detail));
        System.out.printf("[%s] order=%s %s%n", stage, id, detail);
    }
    // Return a copy of the recent steps for observation.
    public synchronized List<Entry> entries() {
        return List.copyOf(entries);
    }
}
