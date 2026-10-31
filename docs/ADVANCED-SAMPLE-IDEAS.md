# More Spring Boot + Kafka sample-project ideas

All of these ideas are now implemented as separate projects 04–14. See [the project map](ADVANCED-PROJECTS.md) for folders, ports, setup, and verification. Start with the three basic samples first.

| Suggested project | Feature to teach | A small demonstration | Difficulty |
|---|---|---|---|
| Order updates in order | Keys, partitions, consumer groups | Publish CREATED then PAID with the same orderId; run two workers and show per-partition ownership | Easy |
| Duplicate-safe loyalty points | Durable consumer idempotency | Deliver the same eventId twice; a database unique constraint allows points to be added once | Medium |
| Delayed email retry | Non-blocking retry topics | A temporary email failure retries later while later records continue; compare with blocking retry | Medium |
| Orders-per-minute dashboard | Kafka Streams, keys, windows, state stores | Count orders per customer in one-minute windows and show how late events are treated | Medium |
| Transactional event processor | Kafka transactions and read_committed | Consume one input and produce two outputs; force rollback and show neither output becomes visible to read_committed readers | Advanced |
| Reliable order publishing | Transactional outbox and CDC | Save an order and outbox row together, interrupt the publisher, then recover the event without losing the database fact | Advanced |
| Evolving order contracts | Avro/Protobuf and Schema Registry | Add an optional field, then try an incompatible schema; demonstrate the configured compatibility rule | Medium |
| Recoverable customer state | Compacted topic and tombstones | Publish updates for one customer, rebuild latest state, then send a tombstone to represent deletion | Medium |
| Broker failure classroom lab | RF3, ISR, acks=all, min ISR2 | Stop one broker and continue; inspect the new leader and ISR after recovery | Medium |
| Trace a slow consumer | Lag, poll interval, rebalancing | Add a controlled processing delay; observe lag, then share partitions between more app instances | Easy |
| Secured event publishing | TLS, authentication, ACLs | Permit one producer to write orders while a different identity is denied | Advanced |

## Suggested order

1. **Keys and consumer groups**: a small extension of project 1.
2. **Duplicate-safe processing**: a useful next step before adding real business side effects.
3. **Non-blocking retries**: extends project 3 and makes the ordering trade-off visible.
4. **Kafka Streams dashboard**: introduces processing with state and windows.
5. **Transactional outbox**: addresses the database-and-Kafka dual-write problem.

## Boundaries to explain while teaching

- Standard groups share partitions; different groups read independently. Ordered arrival does not automatically mean every external side effect completes in that order.
- Kafka producer idempotence handles supported producer-retry duplicates. Durable business deduplication is a separate concern.
- Kafka transactions cover Kafka records and offsets; an arbitrary database, email system, or payment API is not made exactly once by that setting.
- Retry topics can change processing order. Spring Kafka non-blocking retries have feature restrictions, including not combining them with container transactions; check the pinned framework version before building that exercise.
- Schema Registry and CDC connectors are additional services/components, not built into a bare Kafka broker.
- Compaction is asynchronous. Do not promise all old values disappear immediately.

## Official references

- Spring Kafka retry topics: https://docs.spring.io/spring-kafka/reference/retrytopic.html
- Spring Kafka transactions: https://docs.spring.io/spring-kafka/reference/kafka/transactions.html
- Kafka Streams DSL and windowing: https://kafka.apache.org/documentation/streams/developer-guide/dsl-api/
- Kafka design: https://kafka.apache.org/40/design/design/
- Kafka Connect: https://kafka.apache.org/40/kafka-connect/

Use versioned documentation when implementing. The ideas above describe learning goals; they are not a promise that every latest feature is available in this package's pinned versions.
