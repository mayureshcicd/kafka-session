[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 09. Transactional outbox with real Debezium CDC

A PostgreSQL transaction saves an order and an outbox row together. Debezium reads committed changes from the PostgreSQL WAL and publishes them to Kafka.

This is a separate Spring Boot project with its own POM, main class, topics, and port **18089**. Java comments explain why the important lines exist. No other sample application's Java code is needed.

## Start the supporting services

First, from `KAFKA-SESSION`, start the shared Kafka broker:

```bash
docker compose up \
  -d --wait
```

Then from this project directory:

```bash
docker compose up -d
```

This starts PostgreSQL on 25432 and Kafka Connect on 28083, attached to the shared broker network. Start the application below so its database tables exist. **After the app starts**, run `python3 register-connector.py` from this directory. Check `curl localhost:28083/connectors/sample-orders-outbox/status`: connector and task should both be RUNNING.


## Build and run

Use Java 17+, Maven, and Docker Compose v2. First build/image download needs Internet. From **this project directory**, in another terminal if necessary:

```bash
mvn clean package
java -Xmx256m -jar target/outbox-cdc-1.0.0.jar
```

Keep the app running. Do not rebuild or delete its JAR while it is running; stop it with Ctrl+C first. SERVER_PORT overrides the default port. KAFKA_BOOTSTRAP_SERVERS changes the shared broker address except in the security project, which has its own fixed TLS lab configuration.

## Try the feature

```bash
curl -s -X POST localhost:18089/orders \
  -H 'Content-Type: application/json' \
  -d '{"product":"Book","fail":false}'
curl -s localhost:18089/database-counts
curl -s localhost:18089/results
```

Expected: The database counts rise together. A cdc-event entry eventually appears for the returned eventId.

All processing is asynchronous. Use returned IDs to match your request to the last 200 in-memory /results entries. Older Kafka records may replay according to group bookmarks. A 202 confirms the write was accepted, not that business work finished.

## Read and explain the code

Suggested order: **OrderStorage.java -> schema.sql -> OutboxOrders.java -> connector.json**. Application.java starts Spring Boot; DemoLog and ResultController provide a small teaching journal.

This uses actual CDC, not an application polling loop. Raw Debezium envelopes remain visible, and the listener extracts after.payload. Stop Connect, create more orders, and restart Connect: committed outbox rows remain recoverable while the slot/WAL and connector offsets are retained. Consumers still need idempotency. The lab uses a privileged database identity for setup; production should restrict permissions and monitor replication slots/WAL growth.

## Check the result manually

Use the curl requests above, then check /results for the returned ID. Compare the observed steps with the expected success and failure behavior. For multiple app instances, inspect the results from each instance.

## Stop and reset

Stop Java with Ctrl+C. Stop project-specific containers using `docker compose down` from this project if it has its own compose.yaml. Stop the shared broker only after any dependent Registry/Connect services: run `docker compose down` from KAFKA-SESSION. Named Docker volumes retain lab data; adding `-v` deliberately erases that Compose project's volumes.

Local H2/Streams files, where used, live under this project's data/ directory. Delete them only with the app stopped and only if you want an intentional reset. These are classroom samples, not production deployments. See ../docs/ADVANCED-PROJECTS.md and ../docs/ADVANCED-VALIDATION.md for the complete map and actual verification results.

## Prove database rollback and CDC catch-up

```bash
curl -s localhost:18089/database-counts
curl -i -X POST localhost:18089/orders \
  -H 'Content-Type: application/json' \
  -d '{"product":"Rolled back","fail":true}'
curl -s localhost:18089/database-counts
# The counts are unchanged after the deliberate HTTP 500.

docker compose stop connect
curl -s -X POST localhost:18089/orders \
  -H 'Content-Type: application/json' \
  -d '{"product":"Wait for CDC","fail":false}'
docker compose start connect
# After connector recovery, /results includes the returned eventId.
```

Retained Kafka Connect offsets and the PostgreSQL slot/WAL enable recovery; deleting volumes or losing required WAL changes the recovery situation. Do not leave a stopped slot accumulating WAL indefinitely.
