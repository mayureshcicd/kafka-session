package com.example.schemas;


import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Makes the classroom results easy to check without searching large Kafka logs. */
@RestController
public class ResultController {
    private final DemoLog log;

    // Spring supplies the same DemoLog object that our listeners use.
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public ResultController(DemoLog log) {
        this.log = log;
    }

    @GetMapping("/results")
    // Show this app instance's recent results so the learner can follow a message step by step.
    public List<DemoLog.Entry> results() {
        // This is an in-memory teaching log, not a database or Kafka health check.
        return log.entries();
    }
}
