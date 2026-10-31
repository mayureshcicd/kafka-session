package com.example.kafkademo;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.*;
@RestController
@RequestMapping("/demo")
/** Small teaching component used to show Kafka message flow in this sample. */
public class DemoController {
    private final DemoJournal journal;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public DemoController(DemoJournal journal) {
        this.journal = journal;
    }
    @GetMapping("/status")
    // Show this app instance's recent steps for the teaching demonstration.
    public List<DemoJournal.Entry> status() {
        return journal.entries();
    }
    @GetMapping("/health")
    // Confirm the HTTP app responds. This alone does not prove Kafka is healthy.
    public Map<String, String> health() {
        return Map.of("status", "UP", "scope", "application only");
    }
}
