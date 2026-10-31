# Official references and version notes

Prepared 31 October 2026. The lab pins versions to make the session repeatable; it does not claim they are the newest releases. Review dependency support and security updates before adapting it for production.

- Apache Kafka introduction: https://kafka.apache.org/40/getting-started/introduction/
- Kafka design: https://kafka.apache.org/40/design/design/
- Kafka configuration: https://kafka.apache.org/40/configuration/
- Kafka operations: https://kafka.apache.org/40/operations/
- Kafka 4.0 upgrade notes, including ZooKeeper removal: https://kafka.apache.org/40/getting-started/upgrade/
- Official Kafka Docker image and examples: https://github.com/apache/kafka/tree/4.0.0/docker
- Spring Kafka error handling and DeadLetterPublishingRecoverer: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
- Versioned Spring Kafka 3.2.4 reference: https://docs.spring.io/spring-kafka/docs/3.2.4/reference/html/
- Spring Boot managed dependencies: https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html
- Kafka Streams: https://kafka.apache.org/40/streams/
- Kafka Connect: https://kafka.apache.org/40/kafka-connect/

Key factual boundaries: KRaft is the current metadata architecture; ZooKeeper belongs to older deployments. DLQ is a consumer/framework pattern. Schema Registry is a separate ecosystem service. Standard consumer groups share partitions; new version-specific share-group behavior is outside this session. Exactly-once statements must specify their Kafka transaction boundary.
