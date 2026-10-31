# Three independent applications — verification

Verified on 31 October 2026 with Java 17, Maven, and the supplied Apache Kafka 4.0.0 single-broker KRaft lab.

| Check | Result |
|---|---|
| Independent Maven projects | Each has its own Boot parent, artifact, main class, configuration, and topics |
| Build | All three packaged successfully using the root Maven aggregator |
| Pub/sub, port 18081 | A single message reached both email and audit consumer groups |
| Event driven, port 18082 | Order-created triggered inventory, which published stock-reserved and triggered shipping |
| DLQ successful input, port 18083 | One processing attempt and payment-success |
| DLQ failed input, port 18083 | Three attempts, then a DLT record with original FAIL value and error information; no payment-success |
| Java explanations | All 23 Java classes/records contain purpose comments; configuration explains the reason for important settings |
| HTTP/Kafka integration checks | All three live checks passed before the optional helpers were removed |

The script observes actual asynchronous HTTP/Kafka behavior using unique returned IDs. It is an integration check, not a claim of unit-test coverage. No extra test framework or pip packages are required.

The default ports avoid a service already using 8081. Verification did not stop or modify that service. Each sample's SERVER_PORT setting is overridable.

The three samples are independent applications. Inside the event-driven sample, inventory and shipping remain logical components in one process for easy teaching; they are not separately deployed microservices. The basic samples use one Kafka data replica and non-transactional processing. Business actions are simulated by logs. Crashes can cause repeated processing; a production design needs durable idempotency, security, and recovery policies.

The original combined demo and its earlier validation remain in demo/ and docs/VALIDATION.md. They are not dependencies of these three projects. Advanced projects in ADVANCED-SAMPLE-IDEAS.md are suggestions, not additional implemented applications.
