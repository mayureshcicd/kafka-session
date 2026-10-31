[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 04. Keys, partitions, and consumer groups

Publish CREATED and PAID for the same order ID. Both records use that ID as the key. Kafka places them in the same partition, and the listener shows their increasing offsets.

This is a separate Spring Boot project with its own POM, main class, topics, and port **18084**. Java comments explain why the important lines exist. No other sample application's Java code is needed.

## Start the supporting services

First, from `KAFKA-SESSION`, start the shared Kafka broker:

```bash
docker compose up \
  -d --wait
```


## Build and run

Use Java 17+, Maven, and Docker Compose v2. First build/image download needs Internet. From **this project directory**, in another terminal if necessary:

```bash
mvn clean package
java -Xmx256m -jar target/ordering-and-groups-1.0.0.jar
```

Keep the app running. Do not rebuild or delete its JAR while it is running; stop it with Ctrl+C first. SERVER_PORT overrides the default port. KAFKA_BOOTSTRAP_SERVERS changes the shared broker address except in the security project, which has its own fixed TLS lab configuration.

## Try the feature

```bash
curl -s -X POST localhost:18084/orders/order-101/events \
  -H 'Content-Type: application/json' \
  -d '{"status":"CREATED"}'
curl -s -X POST localhost:18084/orders/order-101/events \
  -H 'Content-Type: application/json' \
  -d '{"status":"PAID"}'
curl -s localhost:18084/results
```

Expected: The returned partition numbers match, and the PAID offset is larger. Results show CREATED before PAID for order-101.

All processing is asynchronous. Use returned IDs to match your request to the last 200 in-memory /results entries. Older Kafka records may replay according to group bookmarks. A 202 confirms the write was accepted, not that business work finished.

## Read and explain the code

Suggested order: **OrderUpdates.java -> Topics.java**. Application.java starts Spring Boot; DemoLog and ResultController provide a small teaching journal.

For work sharing, run another instance with SERVER_PORT=18184. Both use sample-ordering-workers, so inspect /results on both instances. Describe that group through kafka-consumer-groups.sh to see assignment. This standard group can have at most three active members because the topic has three partitions. Ordering is inside one partition, not across the whole topic. Changing partition count can change key mapping.

## Check the result manually

Use the curl requests above, then check /results for the returned ID. Compare the observed steps with the expected success and failure behavior. For multiple app instances, inspect the results from each instance.

## Stop and reset

Stop Java with Ctrl+C. Stop project-specific containers using `docker compose down` from this project if it has its own compose.yaml. Stop the shared broker only after any dependent Registry/Connect services: run `docker compose down` from KAFKA-SESSION. Named Docker volumes retain lab data; adding `-v` deliberately erases that Compose project's volumes.

Local H2/Streams files, where used, live under this project's data/ directory. Delete them only with the app stopped and only if you want an intentional reset. These are classroom samples, not production deployments. See ../docs/ADVANCED-PROJECTS.md and ../docs/ADVANCED-VALIDATION.md for the complete map and actual verification results.
