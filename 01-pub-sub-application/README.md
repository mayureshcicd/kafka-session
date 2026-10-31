[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 1. Publish / subscribe — one message, two subscribers

```text
POST /messages -> sample.pubsub.messages -> EmailSubscriber [group: sample-pubsub-email]
                                        -> AuditSubscriber [group: sample-pubsub-audit]
```

Goal: publish one message and show that two independent groups both read it.

## Requirements and Kafka startup

Use Java 17+, Maven, and Docker Compose v2. Start the shared local Kafka from `KAFKA-SESSION`:

```bash
docker compose up -d --wait
```

Kafka uses KRaft; no ZooKeeper is needed. Topics have three partitions and replication factor 1. These samples target the single-broker lab. Each project creates its own topics; it does not depend on either of the other projects.

The project pins Spring Boot 3.3.5 to match the existing training materials. It is a teaching version, not a claim of the latest release. Use `KAFKA_BOOTSTRAP_SERVERS` to change the broker address and `SERVER_PORT` to change the HTTP port.

If JAVA_HOME is invalid, set it to your installed JDK. On the preparation machine: `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`.

## Run this application

From `KAFKA-SESSION/01-pub-sub-application`:

```bash
mvn clean package
java -jar target/pub-sub-application-1.0.0.jar
```

Send a message from another terminal:

```bash
curl -i -X POST http://localhost:18081/messages \
  -H 'Content-Type: application/json' \
  -d '{"message":"Hello Kafka"}'
curl -s http://localhost:18081/results
```

Expected: HTTP 202 returns an `id`. After asynchronous processing, results contain both `email` and `audit` entries for that ID. Their order can vary. Neither subscriber forwards the message to the other.

## Read the Java files in this order

1. `Application`: starts Spring Boot and discovers components.
2. `TopicConfiguration`: creates this example's three-partition topic.
3. `MessageController`: validates text, generates an ID, and publishes with KafkaTemplate.
4. `EmailSubscriber`: subscribes with the email group ID.
5. `AuditSubscriber`: uses a different group ID to receive the same record independently.
6. `DemoLog` and `ResultController`: let the presenter show outcomes through `/results`.

Every class includes comments about the purpose of the code. Producer values are plain strings to avoid introducing JSON event conversion in this first lesson.

## Extra exercise: same group shares work

Run a second instance on a different port:

```bash
SERVER_PORT=18181 java -jar target/pub-sub-application-1.0.0.jar
```

Both instances use the same email group and the same audit group. Members of each group share three partitions; inspect results on **both** 18081 and 18181. Each group still independently reads the full stream. Use the returned ID to find the result in the appropriate instance.

## Teaching boundaries

The last 200 results are held in memory and lost when this app restarts. Kafka data and committed offsets remain in the named Docker volume. Reading does not delete Kafka records. Existing groups resume their bookmarks even though auto.offset.reset is earliest. Use unique IDs from each POST to distinguish new results from retained history.

No real email, stock change, shipping, or payment occurs. Processing is non-transactional and can repeat after a crash; production business actions need durable idempotency. These projects are separate deployable apps, with logical handlers inside each app. The event-driven handlers can be split into independently deployed services in a later exercise.

Stop the app with Ctrl+C. Stop Kafka from `KAFKA-SESSION` using `docker compose down`. Adding `-v` deletes the lab's retained data, so use that only for an intentional reset. Do not run these on the same ports as the older combined demo's multi-instance exercise.
