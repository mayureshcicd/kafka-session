# Kafka handbook in easy language

## 1. Why do we need Kafka?

Imagine an order service directly calls email, inventory, billing, and reporting. Every new receiver adds another connection. Slow receivers can slow down the order request. A temporary failure can make a chain of calls fail.

Kafka gives us a shared event journal. The order service writes a fact once. Other services read it when ready. Kafka retains the fact for a configured period, so a reader can catch up or replay retained history.

Benefits include independent services, handling traffic bursts, parallel processing, multiple readers, replay, and replication. Costs include more operations, storage, monitoring, and eventual consistency. Kafka is useful for event streams; a direct HTTP call can be simpler when a caller needs an immediate answer.

## 2. The main words

| Term | Easy meaning | Example |
|---|---|---|
| Event | A fact that happened | An order was created |
| Record | The data Kafka stores | Key, JSON value, timestamp, headers |
| Producer | An application that writes records | Order API |
| Consumer | An application that reads records | Inventory listener |
| Topic | A named stream of related records | orders |
| Partition | One ordered log within a topic | orders partition 0 |
| Offset | A position inside one partition | 0, 1, 2, 3... |
| Broker | A running Kafka data server | kafka1 |
| Cluster | Brokers working together | kafka1, kafka2, kafka3 |
| Consumer group | A team sharing reading work | inventory-service |
| Replica | A copy of a partition on another broker | P0 copy on kafka2 |
| Leader | Replica that normally handles a partition's requests | P0 leader on kafka1 |
| Follower | Replica that copies its leader | P0 follower on kafka2 |
| ISR | In-sync replicas caught up enough to count | kafka1 and kafka2 |
| Lag | Distance between a group's bookmark and the log end | 100 minus 90 = 10 |
| Controller | Manages metadata and leader assignments | KRaft controller |
| Rebalance | Changes partition assignments among readers | A second app joins |
| DLQ or DLT | A topic for records that could not be handled | orders.DLT |
| Serialization | Convert an object to bytes | OrderEvent to JSON |
| Deserialization | Convert bytes back into an object | JSON to OrderEvent |

## 3. How Kafka works internally

1. The producer serializes the key and value into bytes.
2. It asks brokers for metadata: topics, partitions, and leaders.
3. It selects a partition. With a normal key-based partitioner, the same key generally maps to the same partition while the partition count and partitioner remain unchanged.
4. It batches records and sends them to that partition's leader.
5. The leader appends records to its log. Followers fetch from the leader to replicate the data.
6. The producer receives success or failure according to acknowledgement and timeout settings.
7. A consumer polls records and runs application code.
8. The consumer group commits progress. After a restart, the group uses its committed bookmark.

Brokers store partition logs in segment files. Indexes help find positions. Sequential writes, batching, compression, and the operating system page cache make the data path efficient. Kafka does use disk; it is not only an in-memory message bus. Acknowledgement does not mean every replica performed a physical disk sync for that record.

Consumers normally read from leaders. Reading does not delete records. A broker can hold partitions belonging to many topics. Each partition has one leader at a time; a cluster can have many partition leaders spread across brokers.

## 4. Topics, keys, partitions, and ordering

A topic can have several partitions. Each is an independent ordering lane:

```text
orders P0: [offset 0] [offset 1] [offset 2]
orders P1: [offset 0] [offset 1]
orders P2: [offset 0] [offset 1] [offset 2] [offset 3]
```

There is no global offset across all partitions and no total topic order. Use the business key that matches the order you need: orderId for one order's lifecycle, customerId for a customer's stream. Our demo keys records by orderId.

One producer's ordinary ordered sends to a partition are ordered there; application parallelism, retries, multiple producers, and external side effects still need careful reasoning. Idempotent producer settings help preserve safety during retries.

More partitions can permit more workers, but they also add metadata, files, replication work, and rebalance cost. Increasing partition count can change key mapping. One hot key still belongs to one ordering lane and can become a bottleneck. You cannot normally reduce a topic's partition count in place.

## 5. Pub/sub and consumer groups

Publish/subscribe means producers publish to a topic and interested readers subscribe. A producer need not know its readers.

Different group IDs have independent progress. Notifications and inventory both read the same orders. They are not competing with each other.

Within one standard consumer group, members divide partition ownership. With three partitions and two consumers, one may receive two partitions and the other one partition. With four consumers, at most three are active. One partition is assigned to one group member at a time in the standard model used in this lab. Redelivery can still happen after failure.

A group coordinator and the chosen group protocol manage membership and assignment. Member joins, exits, or failures can trigger rebalancing. Consumer protocol details vary by Kafka version. Newer share-group features have different behavior and are beyond this lab.

## 6. Offsets, commits, and lag

After processing offset 4, commit 5: the next position to read. Kafka stores group offsets in the compacted internal topic `__consumer_offsets`. Consumers also have a current in-memory position that can be ahead of their committed position.

- Commit before processing: a crash can skip unfinished work.
- Process then commit: a crash between those actions can repeat work.
- Ordinary reliable processing therefore usually needs idempotency.

`enable.auto.commit=false` lets the Spring listener container manage commits. Our demo uses record acknowledgement: commit after handling a record or successfully routing it through configured recovery. Offset commit and real external work are not one atomic action here.

`auto.offset.reset=earliest` means start at the earliest available position when there is no valid committed position. It does not rewind an existing group every time it restarts. `latest` starts near the end in that fallback situation.

A simple committed lag example: log end is 100 and committed position is 90, so lag is 10 positions. Watch event age and processing latency too. Compaction, retention, and transactions mean this number need not equal exactly ten pending business events.

If retention deletes records before a slow reader catches up, those records cannot be recovered from that topic. Lag is an operational signal, not a guarantee that history is still available.

## 7. One broker, several brokers, and several app instances

These are different dimensions:

| Choice | Changes what? | Does it give broker failover? |
|---|---|---|
| One broker, RF1 | Simple storage setup | No |
| Three brokers, RF1 | More servers, but no data copies for each partition | Not for partitions whose only replica is lost |
| Three brokers, RF3 | Three copies per partition | Can, with healthy ISR and controller quorum |
| Two application instances | Consumer work sharing within each group | No; this affects applications |
| Different group IDs | Independent reading progress | No; this affects subscriptions |

A replica is not a consumer. Adding consumers does not make extra broker copies. Adding brokers does not automatically increase every existing topic's replication factor or rebalance all existing data; use partition reassignment tooling when required.

Our single-broker lab still has three partitions so it can show consumer sharing. Its replication factor is 1. Our separate three-broker lab creates fresh RF3 topics. The environment variable changes topic creation, not an existing topic's replication factor.

## 8. Replication, leaders, followers, and acknowledgements

Replication factor counts all copies, including the leader. RF3 means one leader and two followers. Kafka normally places these on different brokers.

- `acks=0`: producer does not wait for a broker response.
- `acks=1`: leader acknowledges its write; follower replication may still be pending.
- `acks=all`: leader waits for all current ISR replicas required by the replication protocol.
- `min.insync.replicas=2`: with acks=all, reject writes when fewer than two replicas are in sync.

A useful teaching configuration is three brokers, RF3, min ISR2, acks=all. If one broker fails and the other two are healthy, eligible replicas and the controller majority allow recovery. If only one ISR remains, rejecting writes protects durability at the expense of availability. RF3 alone does not make this promise.

ISR is not “all replicas that exist.” A slow follower can leave ISR and catch up later. Allowing an out-of-sync replica to lead can lose records; understand unclean leader election before enabling it. Replication reduces some failure risks, but correlated host failures, bad retention settings, or operator actions still need recovery planning.

## 9. ZooKeeper and KRaft

Older Kafka deployments used a separate ZooKeeper ensemble for coordination and metadata. Kafka 4.0 removed ZooKeeper mode. Modern Kafka uses KRaft, where a Raft controller quorum maintains a replicated metadata log.

Controllers manage metadata and partition-leader changes. Partition leaders handle partition data. These roles are different. A controller quorum needs a majority; three controllers tolerate one controller loss, and five tolerate two, assuming healthy remaining nodes.

The labs combine broker and controller roles to stay small. Production commonly uses dedicated controllers and spreads nodes across failure domains. Three Docker containers on the same laptop cannot survive losing that laptop.

## 10. Event-driven architecture

An event-driven architecture connects services through events. Services react to facts and can publish new facts:

```text
OrderCreated -> inventory reaction -> StockReserved -> shipping reaction
             -> notification reaction
             -> reporting reaction
```

Benefits: loose coupling, independent scale, separate progress, and the ability to add readers. Challenges: eventual consistency, duplicate processing, out-of-order business arrival, schema changes, tracing, and recovery.

Use meaningful event names, unique event IDs, timestamps, correlation IDs, and clear ownership. Define schema compatibility. Use the correct key for ordering. Implement business idempotency using a unique constraint or durable processed-event tracking; an in-memory set is not a durable production solution.

The database dual-write problem occurs when saving a row and publishing an event are separate actions. A crash between them can leave inconsistent results. A transactional outbox saves business state and an outbox row in one database transaction; a relay or CDC connector later publishes the row. Duplicate delivery is still possible, so consumers need idempotency.

For several business steps, a saga coordinates progress and compensation. For example, release reserved stock if payment fails. Compensation is a new business action, not a magical distributed database rollback. Event sourcing, which treats events as the source of application state, is an optional architectural pattern; using Kafka does not automatically make an application event sourced.

## 11. Retries and DLQ

A DLQ means dead-letter queue. In Kafka it is usually a normal topic, also called DLT. Kafka brokers do not decide that your Java method failed and move its record automatically.

A consumer framework can catch the failure, retry according to policy, publish the failed record to another topic, and advance source progress only after successful recovery.

The sample uses an inventory-specific `DefaultErrorHandler`, `FixedBackOff(1000, 2)`, and `DeadLetterPublishingRecoverer`. That means one original attempt plus two retries. It routes the original partition to the same partition number in `orders.DLT`, so both topics have three partitions.

The recoverer checks the publishing result. If DLT publishing fails, it throws and recovery is not considered complete. Source commit and DLT publishing are not transactional here, so a crash between them may duplicate a DLT record.

Temporary failures may improve with backoff. Permanent bad data usually should not be retried unchanged. The invalid amount in the lab intentionally repeats to make the behavior visible. Frameworks also classify some exception types as fatal, so not every error automatically follows the same retry count.

Blocking retry delays work on a partition. Retry-topic approaches defer failed events to other topics but can change ordering. Deserialization failures happen before business code: handling them needs a configured `ErrorHandlingDeserializer` and a recoverer/serializer that can preserve raw failed data. This particular demo focuses on valid JSON that fails business processing.

A useful DLQ includes the original payload, original key, source topic/partition/offset, exception context, and correlation ID. Protect sensitive data, set retention, monitor growth, and assign an owner. Fix the underlying problem before replaying. Replaying to the original topic may notify all its groups again, so select a recovery route deliberately.

## 12. Delivery guarantees and transactions

At-most-once permits loss but avoids repeated delivery under the chosen processing approach. At-least-once permits repeated delivery and needs idempotent business actions. Exactly-once must always state the system boundary.

Producer idempotence prevents duplicate Kafka writes caused by producer retries under its supported settings. It does not notice that a user pressed Submit twice or that a restarted workflow published the same business event again.

Kafka transactions can publish records and consumed offsets atomically in Kafka-to-Kafka processing. Downstream consumers use `isolation.level=read_committed` to avoid aborted data. They do not make a payment provider, email system, and relational database part of one Kafka transaction.

The sample is non-transactional and at least once. Inventory publishes its next event before the source offset commit. A crash between them can repeat that next event. No actual email, stock mutation, or shipment is performed.

## 13. Retention, compaction, and tombstones

With delete retention, old segments are removed according to time or size settings, even if consumers have not read them. Cleanup is asynchronous and segment based; retention is not an exact timer for each individual record.

With compaction, Kafka eventually preserves the latest value per key, useful for latest-state streams. Old versions can remain until cleaning. Offsets do not get renumbered, so gaps can appear. A keyed record with a null value is a tombstone that marks deletion; its retention must allow readers to notice it. Compaction can be combined with delete retention.

Estimate retained payload storage as incoming bytes per second multiplied by retention seconds and replication factor. Add overhead, indexes, compression considerations, headroom, and recovery capacity. Some deployments use tiered storage; availability and behavior depend on the version, remote storage implementation, and configuration.

## 14. Advanced features and ecosystem

| Feature | Simple purpose | Important boundary |
|---|---|---|
| Kafka Connect | Move data into and out of Kafka | Connectors are installed and operated separately |
| CDC | Publish database changes | Often supplied by a connector such as Debezium |
| Kafka Streams | Java filtering, joins, counts, and aggregates | Runs inside an application, not a broker |
| Windowing | Group events over time | Decide event time, late arrival, and grace periods |
| State stores/changelogs | Keep and recover streaming state | Repartitioning and restore time matter |
| Idempotent producer | Avoid duplicates from producer retries | Does not deduplicate business actions |
| Transactions | Atomic Kafka record/offset updates | Does not include arbitrary external side effects |
| Schema Registry | Store schemas and enforce compatibility | Separate service, not part of the Kafka broker |
| MirrorMaker 2 | Replicate between Kafka clusters | Usually asynchronous; plan offsets and failover |
| Quotas | Limit client resource use | Helps fairness; does not replace capacity planning |
| Rack awareness | Place replicas across failure domains | Requires correct placement and configuration |
| New group protocols | Improve coordination behavior | Support and semantics depend on client/server versions |

For schema evolution, compatible optional additions are generally easier than removing or changing fields. Exact rules depend on serialization format and compatibility mode. Test both older and newer consumers. A schema version field alone does not enforce compatibility.

## 15. Security, monitoring, and tuning

TLS encrypts network traffic. SASL or client certificates establish identity. ACLs authorize operations. These solve different problems. Secure the network, credentials, topic permissions, logs, and DLQ payloads.

Monitor consumer lag, oldest event age, processing time, error/retry/DLQ rate, group changes, disk usage, offline partitions, ISR shrinkage, request latency, and controller health. A zero-lag DLQ pipeline can still mean business failure, so monitor outcomes as well as queue positions.

Producer batching, linger, and compression can improve throughput but affect latency and resources. Consumer fetch settings and poll sizes influence batching. If handling a poll takes too long, `max.poll.interval.ms` can be exceeded and cause reassignment. Fix slow work and capacity first; do not blindly increase timeouts.

Add consumers only when partitions and resources allow it. Add brokers for storage/network capacity and redistribute partitions intentionally. Watch hot keys. Load test with realistic payloads and failure scenarios.

## 16. Replay and disaster recovery

A new group can read retained history without changing another group's bookmarks. Resetting an existing group's offsets is a separate administrative operation; stop its members first and use a dry run before executing a reset. Do not rewind a payment or email consumer casually.

Records removed by retention cannot be replayed from that topic. Compacted history may omit earlier values. In-cluster replicas are not a substitute for backup or cross-region recovery. MirrorMaker 2 can help replicate across clusters, but asynchronous replication has recovery-point and recovery-time trade-offs. Practice recovery rather than assuming it works.

## 17. Questions to rehearse

1. Does Kafka delete a record after it is read? **No; retention/cleanup policies control storage.**
2. Do two different groups both read the order? **Yes, independently, while it is available.**
3. Can four standard consumers all work with three partitions? **At most three are active in that group.**
4. Is offset 10 globally unique? **No; it is meaningful within a partition.**
5. After processing offset 4, what do we commit? **5, the next position.**
6. Does Kafka automatically move Java failures to a DLQ? **No; configure consumer recovery.**
7. Does one broker provide high availability? **No broker redundancy.**
8. Does acks=all mean a fixed two copies? **No; it depends on the current ISR, with min ISR enforcing the minimum.**
9. Is KRaft a misspelling of ZooKeeper? **No; it is the replacement metadata architecture.**
10. Does Kafka exactly once make email exactly once? **No; external side effects need their own safe design.**
11. Why can notification succeed when inventory fails? **They use independent consumer groups.**
12. Why does restarting not reread everything with earliest? **A valid existing committed offset takes precedence.**
13. Are partitions and replicas the same? **No; partitions split data, replicas copy it.**
14. Does adding a broker automatically change existing RF1 topics to RF3? **No.**
15. What do you do with a DLQ? **Inspect, fix, validate, replay safely, and monitor.**

See SOURCES.md for official references. This handbook is a broad beginner-to-intermediate training guide; version-specific APIs and full production runbooks need the corresponding release documentation.
