# Demo guide

Run all Docker commands from `KAFKA-SESSION`. Run Maven from `KAFKA-SESSION/demo`. Docker commands refer only to the named Kafka session projects.

## 1. Single broker

```bash
docker compose up -d --wait
docker compose ps
cd demo
mvn clean package
java -jar target/kafka-training-demo-1.0.0.jar
```

Wait for the application to start and Kafka listener partitions to be assigned. KafkaAdmin creates `orders`, `inventory.reserved`, and `orders.DLT`, each with three partitions and replication factor 1. The DLQ has matching partition count because failed records retain their source partition number.

If Maven complains about JAVA_HOME, point it to your installed Java 17+ JDK. For the machine used to prepare this package:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

`GET /demo/health` only confirms the HTTP app is running. It is not a Kafka connectivity check. A successful acknowledged POST and the smoke test confirm more of the path.

## 2. Pub/sub: one record, two independent groups

```bash
curl -i -X POST http://localhost:8080/orders   -H 'Content-Type: application/json'   -d '{"customerId":"customer-1","amount":25.50}'
```

Expected HTTP status: 202. Copy the returned `orderId`. You should see `notification`, `inventory-attempt`, `inventory-success`, and `shipping` journal stages for it. The order of logs across groups is not guaranteed.

Explain: notifications and inventory use different group IDs, so both read the same `orders` record. Kafka does not need two copies of that source record. If the broker cannot acknowledge the send, the endpoint does not return a successful 202.

## 3. Event-driven chain

Inventory does not call ShippingListener as a Java method or make an HTTP request to it. It publishes the event to `inventory.reserved`. Shipping consumes that topic in `shipping-service`.

```text
orders -> InventoryListener -> inventory.reserved -> ShippingListener
```

The REST endpoint knows only `orders`. It does not wait for inventory or shipping. Logs simulate the business actions. The app reuses the small OrderEvent payload for both topics; production would normally have explicit event types, schema versions, and event IDs.

## 4. Failure, retries, and DLQ

```bash
curl -i -X POST http://localhost:8080/orders   -H 'Content-Type: application/json'   -d '{"customerId":"customer-2","amount":0}'
```

Expected for this order:

1. `notification` still appears.
2. `inventory-attempt` appears three times: initial attempt plus two retries, about one second apart.
3. `DLQ` appears, with the original event. The log decodes source-topic and exception-message headers; the retained DLT record includes additional Spring Kafka failure headers.
4. No `inventory-success` and no `shipping` entry for that failed order.

The retry delay blocks progression for the failed partition; other groups continue independently. We retry an invalid amount purely to show the retry counter. Real systems should usually classify invalid input as permanent and avoid useless retries.

Inspect the journal:

```bash
curl -s http://localhost:8080/demo/status
curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' -d '{"customerId":"manual-check","amount":25.50}'
curl -s http://localhost:8080/demo/status
```

Match each returned orderId to the journal. Check that a valid order reaches notification, inventory-success, and shipping. A failed order must show three inventory attempts and DLQ, with no shipping for that order.

Inspect retained DLQ records directly:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh   --bootstrap-server kafka:19092 --topic orders.DLT --from-beginning   --property print.key=true --property print.headers=true --timeout-ms 10000
```

The console command may end with a timeout after printing available data; that is expected. Reading does not delete records. Investigating, fixing and replaying are separate operational steps. This sample deliberately does not auto-replay invalid orders.

## 5. Inspect partitions, replicas, and lag

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh   --bootstrap-server kafka:19092 --describe --topic orders

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server kafka:19092 --describe --group inventory-service
```

Point to Partition, Leader, Replicas, and Isr in the topic output. In the group output point to CURRENT-OFFSET, LOG-END-OFFSET, and LAG. Low/zero committed lag after recovery means the record was handled or routed through recovery, not that every business operation succeeded.

## 6. Multiple Spring Boot instances

Keep the first app running on 8080. From `demo/`, launch another terminal:

```bash
SERVER_PORT=8081 java -jar target/kafka-training-demo-1.0.0.jar
```

Both applications use the same four group IDs. Each listener has concurrency 1. Partitions are divided between the two members in each group. For each `orders` record, one notification member and one inventory member process it. They may be in different JVMs.

Send several orders, then inspect both local journals:

```bash
curl -s http://localhost:8080/demo/status
curl -s http://localhost:8081/demo/status
```

Use the `kafka-consumer-groups.sh --describe --group inventory-service --members --verbose` command to see assignments. Three partitions allow at most three active members in this standard group. Inspect both journals when two apps are running.

## 7. Three-broker KRaft cluster

Stop both Java apps first. The cluster has a different Compose project and volumes, so it does not reuse the single-broker topics.

```bash
docker compose down
docker compose -f compose.cluster.yaml up -d --wait
cd demo
KAFKA_BOOTSTRAP_SERVERS=localhost:29092,localhost:29093,localhost:29094 DEMO_REPLICATION_FACTOR=3 java -jar target/kafka-training-demo-1.0.0.jar
```

Each broker is also a controller in this teaching lab. The three controllers form a majority quorum. Each topic has RF=3; cluster min.insync.replicas=2 and producer acks=all protect writes when one broker fails.

From `KAFKA-SESSION`:

```bash
docker compose -f compose.cluster.yaml exec kafka1 /opt/kafka/bin/kafka-topics.sh   --bootstrap-server kafka1:19092 --describe --topic orders

curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' -d '{"customerId":"manual-check","amount":25.50}'
curl -s http://localhost:8080/demo/status
```

Expected: three replica IDs per partition, and all three in ISR when healthy.

## 8. Broker failure exercise

Do this only in the three-broker lab. The laptop itself remains a single point of failure.

```bash
docker compose -f compose.cluster.yaml stop kafka1
curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' -d '{"customerId":"manual-check","amount":25.50}'
curl -s http://localhost:8080/demo/status

docker compose -f compose.cluster.yaml exec kafka2 /opt/kafka/bin/kafka-topics.sh   --bootstrap-server kafka2:19092 --describe --topic orders

docker compose -f compose.cluster.yaml start kafka1
```

Allow leader election, metadata refresh, and replica catch-up. The two healthy nodes retain a controller majority and enough ISR for writes. A brief delay or retry is normal. Describe topics again and wait for ISR=3 after restarting. If you stop two nodes, controller majority and/or minimum ISR are lost, so availability is no longer expected. Avoid that during the main presentation.

## 9. Important code to show

| Class | Explanation |
|---|---|
| `OrderController` | Publishes and waits asynchronously for broker acknowledgement |
| `KafkaConfiguration` | Topics, matching DLQ partitions, inventory-only retry handler |
| `NotificationListener` | Independent pub/sub group |
| `InventoryListener` | Business processing, deliberate failure, and downstream event |
| `ShippingListener` | Reacts to a successful inventory event |
| `DeadLetterListener` | Logs failed record, partition, offset, and headers |
| `DemoJournal` | Last 200 log entries for easy classroom observation |

The recoverer checks DLQ publish results. If DLQ publishing fails, the source record is not treated as recovered. DLQ publish and source-offset commit are not a single transaction here, so crashes can still produce duplicates. The same limitation applies between downstream event publishing and source commit.

## 10. Troubleshooting

| Symptom | Check |
|---|---|
| Maven says JAVA_HOME is invalid | Set it to an installed JDK, not a missing Java path |
| Docker permission denied | Use your system's approved Docker access setup |
| Port 9092 or 8080 in use | Stop the conflicting app or adjust the lab's port configuration |
| App fails KafkaAdmin startup | Wait for Kafka health; check Docker logs and bootstrap addresses |
| Broker data-directory permission failure | Use the supplied `/var/lib/kafka/data` volume mount; do not reuse a root-owned custom mount |
| No old records after restarting | Existing group resumes its commits; earliest does not reset a valid commit |
| Duplicate work after restart | Expected with at-least-once processing; use business idempotency in real systems |
| Second app appears to miss records | Same-group apps split work; inspect both journals and partition assignments |
| DLQ is empty | Amount must be 0 or negative; wait for three inventory attempts; check the original orderId |
| Random CLI consumer cannot read JSON | Spring's typed consumer expects OrderEvent JSON; use the app API or console consumer |
| Malformed JSON never reaches handler | This demo covers processing exceptions; use ErrorHandlingDeserializer for a poison-data design |

## 11. Stop and reset

```bash
# Stop Java with Ctrl+C, then stop whichever Kafka lab you started:
docker compose down
docker compose -f compose.cluster.yaml down

# Optional fresh start: this deletes only that lab's retained Kafka data.
docker compose down -v
docker compose -f compose.cluster.yaml down -v
```
