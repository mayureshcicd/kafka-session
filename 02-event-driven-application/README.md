[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 2. Event-driven workflow — a fact triggers the next step

```text
POST /orders
     -> sample.events.orders-created
          -> InventoryListener
               -> sample.events.stock-reserved
                    -> ShippingListener
```

Goal: show how a service reacts to a business fact and publishes a new fact. Kafka pub/sub is the communication mechanism; event-driven architecture is how we arrange the business workflow around those events.

For a beginner-friendly setup, the order API, inventory handler, and shipping handler are logical components in this **one** independently runnable application. They communicate through Kafka topics, not direct method calls. They are not three separate microservice processes. They can be extracted into separate deployments later without changing this basic flow.

## Requirements and Kafka startup

Use Java 17+, Maven, and Docker Compose v2. Start the shared local Kafka from `KAFKA-SESSION`:

```bash
docker compose up -d --wait
```

Kafka uses KRaft; no ZooKeeper is needed. Topics have three partitions and replication factor 1. These samples target the single-broker lab. Each project creates its own topics; it does not depend on either of the other projects.

The project pins Spring Boot 3.3.5 to match the existing training materials. It is a teaching version, not a claim of the latest release. Use `KAFKA_BOOTSTRAP_SERVERS` to change the broker address and `SERVER_PORT` to change the HTTP port.

If JAVA_HOME is invalid, set it to your installed JDK. On the preparation machine: `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`.

## Run this application

From `KAFKA-SESSION/02-event-driven-application`:

```bash
mvn clean package
java -jar target/event-driven-application-1.0.0.jar
```

Send an order:

```bash
curl -i -X POST http://localhost:18082/orders \
  -H 'Content-Type: application/json' \
  -d '{"product":"Book","quantity":2}'
curl -s http://localhost:18082/results
```

Expected: HTTP 202 returns `orderId`, product, and quantity. Results eventually include `inventory` and `shipping` for that ID. The request is accepted after Kafka acknowledges the first event. It does not wait for the whole workflow.

## Read the Java files in this order

1. `OrderEvent`: small Java record serialized as JSON. Both topics reuse it to keep the lesson short.
2. `OrderController`: validates input and publishes the order-created fact.
3. `InventoryListener`: reacts, simulates stock reservation, and publishes the stock-reserved fact.
4. `ShippingListener`: reacts only to stock-reserved, without being called directly by inventory.
5. `TopicConfiguration`: creates both topics.
6. `DemoLog` and `ResultController`: show the business steps.

## Why does inventory wait for the send result?

If publishing the second event fails, the listener must fail too so it is not treated as successful. This does not create a transaction: a crash after publishing but before committing the first topic can still repeat the second event. Real stock changes need durable idempotency and an outbox or another consistent design.

This sample has no custom DLQ policy; that is taught separately in project 3. A real system should choose an explicit failure policy rather than rely on framework defaults.

## Teaching boundaries

The last 200 results are held in memory and lost when this app restarts. Kafka data and committed offsets remain in the named Docker volume. Reading does not delete Kafka records. Existing groups resume their bookmarks even though auto.offset.reset is earliest. Use unique IDs from each POST to distinguish new results from retained history.

No real email, stock change, shipping, or payment occurs. Processing is non-transactional and can repeat after a crash; production business actions need durable idempotency. These projects are separate deployable apps, with logical handlers inside each app. The event-driven handlers can be split into independently deployed services in a later exercise.

Stop the app with Ctrl+C. Stop Kafka from `KAFKA-SESSION` using `docker compose down`. Adding `-v` deletes the lab's retained data, so use that only for an intentional reset. Do not run these on the same ports as the older combined demo's multi-instance exercise.
