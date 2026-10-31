[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 3. Retry and DLQ — make failed processing visible

```text
POST /payments -> sample.dlq.payments -> PaymentListener
                                        | success -> payment-success
                                        | failure -> retry twice
                                             -> sample.dlq.payments.DLT -> DeadLetterListener
```

Goal: distinguish “Kafka accepted the record” from “business processing succeeded,” then inspect a failed record. DLQ means dead-letter queue; DLT means dead-letter topic. Kafka does not route Java exceptions automatically. Spring's configured recoverer does that here.

## Requirements and Kafka startup

Use Java 17+, Maven, and Docker Compose v2. Start the shared local Kafka from `KAFKA-SESSION`:

```bash
docker compose up -d --wait
```

Kafka uses KRaft; no ZooKeeper is needed. Topics have three partitions and replication factor 1. These samples target the single-broker lab. Each project creates its own topics; it does not depend on either of the other projects.

The project pins Spring Boot 3.3.5 to match the existing training materials. It is a teaching version, not a claim of the latest release. Use `KAFKA_BOOTSTRAP_SERVERS` to change the broker address and `SERVER_PORT` to change the HTTP port.

If JAVA_HOME is invalid, set it to your installed JDK. On the preparation machine: `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`.

## Run this application

From `KAFKA-SESSION/03-dlq-application`:

```bash
mvn clean package
java -jar target/dlq-application-1.0.0.jar
```

Publish a successful payment reference:

```bash
curl -i -X POST http://localhost:18083/payments \
  -H 'Content-Type: application/json' \
  -d '{"reference":"PAYMENT-101"}'
```

Expected for its returned ID: one `attempt`, then `payment-success`.

Publish a deliberate failure:

```bash
curl -i -X POST http://localhost:18083/payments \
  -H 'Content-Type: application/json' \
  -d '{"reference":"FAIL"}'
curl -s http://localhost:18083/results
```

Expected after about two seconds plus Kafka processing time: three `attempt` entries, one `DLQ` entry with the original value and decoded reason, and **no** `payment-success` for the failed ID. The HTTP response is still 202 because publishing succeeded even though downstream work fails.

The FAIL reference is a teaching switch. It always fails, so retrying it is useful only for demonstration. Real applications distinguish temporary errors worth retrying from permanent errors that need immediate investigation.

## Read the Java files in this order

1. `PaymentController`: publishes a plain string; accepts FAIL on purpose.
2. `PaymentListener`: logs each attempt, throws for FAIL, and logs successful work otherwise.
3. `RetryConfiguration`: adds the payment-only listener factory, limited backoff, and recoverer.
4. `DeadLetterListener`: reads failed records and decodes the error header.
5. `TopicConfiguration`: creates matching three-partition source and DLT topics.
6. `DemoLog` and `ResultController`: show attempt counts and outcomes.

## Important settings in easy words

- `FixedBackOff(1000, 2)`: wait one second between retries; retry twice after the first attempt.
- `DeadLetterPublishingRecoverer`: publishes the original failed record to the DLT with failure headers.
- `setFailIfSendResultIsError(true)`: if the DLT publish fails, do not pretend recovery succeeded.
- Matching partition counts: the DLT uses the same partition number as the original record.
- Separate factory: the DLQ monitor does not accidentally reuse payment retry-to-DLQ routing.
- Record acknowledgement: save source progress after successful processing or successful recovery.

Blocking retry can delay other records in the failed partition. Publishing the DLT and committing source progress are not one transaction, so a crash can duplicate a DLT record. Reading a DLT does not fix or automatically replay it. The sample covers valid strings that fail processing; malformed-byte or JSON-deserialization recovery needs additional configuration.

Inspect retained failures from `KAFKA-SESSION`:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic sample.dlq.payments.DLT \
  --from-beginning --property print.key=true --property print.headers=true \
  --timeout-ms 10000
```

The console consumer can end with a timeout after showing available records. That does not mean the application lost them.

## Teaching boundaries

The last 200 results are held in memory and lost when this app restarts. Kafka data and committed offsets remain in the named Docker volume. Reading does not delete Kafka records. Existing groups resume their bookmarks even though auto.offset.reset is earliest. Use unique IDs from each POST to distinguish new results from retained history.

No real email, stock change, shipping, or payment occurs. Processing is non-transactional and can repeat after a crash; production business actions need durable idempotency. These projects are separate deployable apps, with logical handlers inside each app. The event-driven handlers can be split into independently deployed services in a later exercise.

Stop the app with Ctrl+C. Stop Kafka from `KAFKA-SESSION` using `docker compose down`. Adding `-v` deletes the lab's retained data, so use that only for an intentional reset. Do not run these on the same ports as the older combined demo's multi-instance exercise.
