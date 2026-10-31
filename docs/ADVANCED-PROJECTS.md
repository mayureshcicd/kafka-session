# Advanced Kafka sample projects

All 11 previously suggested ideas now have separate implementations, numbered 04–14. Read a project's README before starting it: not every project uses the same supporting services.

| Project | Port | Extra services |
|---|---|---|
| [04-ordering-and-groups](../04-ordering-and-groups/README.md) — Keys, partitions, and consumer groups | 18084 | Shared Kafka only |
| [05-idempotent-loyalty](../05-idempotent-loyalty/README.md) — Durable duplicate-safe processing | 18085 | Shared Kafka only + embedded H2 file |
| [06-non-blocking-retry](../06-non-blocking-retry/README.md) — Retry topics and delayed recovery | 18086 | Shared Kafka only |
| [07-streams-dashboard](../07-streams-dashboard/README.md) — Kafka Streams windows and materialized state | 18087 | Shared Kafka only |
| [08-kafka-transactions](../08-kafka-transactions/README.md) — Transactional consume-transform-produce | 18088 | Shared Kafka only |
| [09-outbox-cdc](../09-outbox-cdc/README.md) — Transactional outbox with real Debezium CDC | 18089 | PostgreSQL + Debezium Connect |
| [10-schema-evolution](../10-schema-evolution/README.md) — Avro and Schema Registry compatibility | 18090 | Schema Registry |
| [11-compaction-and-tombstones](../11-compaction-and-tombstones/README.md) — Compacted state and tombstones | 18091 | Shared Kafka only |
| [12-broker-failover](../12-broker-failover/README.md) — Replication, ISR, and leader failover | 18092 | Its own three-broker cluster |
| [13-lag-and-rebalancing](../13-lag-and-rebalancing/README.md) — Slow consumers, committed lag, and rebalances | 18093 | Shared Kafka only |
| [14-secure-publishing](../14-secure-publishing/README.md) — TLS client authentication and Kafka ACLs | 18094 | Its own mTLS/ACL broker |

## Build

From KAFKA-SESSION, `mvn clean package` builds projects 01–14. Each project also builds independently. Java 17 and the existing pinned Spring Boot 3.3.5 baseline are used for consistency with the original samples, not as a claim of current production support. Review dependency support/security before adopting the code elsewhere.

Run Java from the individual project directory. This keeps database files, Streams state, and generated certificate paths under that project. Run a few examples at a time rather than starting fourteen JVMs plus all containers on a small laptop.

## Check the examples manually

Start the chosen application and its required services, send the curl requests from its README, and inspect the returned IDs and results. Each README explains the expected success and failure behavior. See ADVANCED-VALIDATION.md for results observed before the optional verification helpers were removed.

## Teaching order

Start with 04 keys/groups and 05 durable idempotency. Continue with 06 retry topics and 11 compaction. Then teach 07 Streams, 08 transactions, and 09 outbox/CDC. Finish with 10 schemas, 12 failover, 13 operational diagnosis, and 14 security.

The goal is to explain one new idea per project. Classroom logs simulate business actions. Kafka transactions do not include arbitrary external side effects. Replay and recovery can repeat work. Separate components such as Registry and CDC must be operated too.

## Source references

- Retry configuration: https://docs.spring.io/spring-kafka/reference/retrytopic/retry-config.html
- Kafka transactions: https://docs.spring.io/spring-kafka/reference/kafka/transactions.html
- Kafka Streams window API: https://kafka.apache.org/37/javadoc/org/apache/kafka/streams/kstream/TimeWindows.html
- Debezium PostgreSQL CDC: https://debezium.io/documentation/reference/3.0/connectors/postgresql.html
- Registry REST API: https://docs.confluent.io/platform/current/schema-registry/develop/api.html
- Schema compatibility: https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- Kafka TLS and ACL configuration: https://kafka.apache.org/40/security/authorization-and-acls/
