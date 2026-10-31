# Validation results

Checked on 31 October 2026 with Java 17, Maven, Docker Engine 29.9.2, Apache Kafka 4.0.0, and Spring Boot 3.3.5.

| Check | Result |
|---|---|
| Maven package | Passed; runnable Spring Boot JAR produced |
| Java tests | 4 passed, 0 failures, 0 errors |
| Single-broker Kafka health | Passed |
| Single-broker pub/sub | Both notifications and inventory received the valid order |
| Event-driven chain | Inventory published inventory.reserved; shipping consumed it |
| Retry and DLQ | Invalid order attempted inventory 3 times, reached notification and DLQ, did not reach shipping |
| Three-broker health | All three brokers healthy |
| Replication | orders: 3 partitions, RF3, min ISR2, all three replicas initially in ISR |
| Two app instances | inventory-service had two members, assigned 1 and 2 partitions respectively |
| Three-broker full workflow | Pub/sub, downstream event, retries, and DLQ all passed |
| Broker failure | Stopped kafka1; full workflow smoke test still passed using remaining brokers |
| PowerPoint | 48 slides, notes present on all slides, shape bounds checked |
| PDF | Rendered successfully to 48 pages with LibreOffice |
| Visual review | Checked pub/sub and three-broker replication diagrams |

## Java test coverage

- HTTP result stays pending until the broker send completes.
- A failed Kafka publish remains a failed request result.
- Invalid inventory input never publishes shipping input.
- A failed downstream event publish propagates for retry and does not log inventory success.

The recorded end-to-end checks used the running broker and HTTP application before the optional helper scripts were removed; unit tests alone do not verify Kafka delivery. The test configuration uses Mockito's subclass mock maker to avoid requiring runtime Java-agent attachment in restricted environments.

## Scope

The sample uses valid OrderEvent JSON and deliberate processing failures. It does not demonstrate malformed-byte recovery, a transactional outbox implementation, a database, real stock/email/shipping services, business deduplication, secure production networking, or exactly-once external side effects. These are explained in the training materials.

Broker failover was tested with one broker loss on one local machine. This does not validate zone or region failure, disaster recovery, or production capacity. DLQ publish and source offset commits are not transactional, so duplicate records remain possible after a crash.

Temporary demo JVMs and Kafka lab containers are stopped after validation. Named Docker volumes retain only the sample lab data for reruns. Use the documented down -v command if a fresh lab is desired.
