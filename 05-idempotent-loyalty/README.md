[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 05. Durable duplicate-safe processing

A database primary key remembers which award event IDs have already been applied. Duplicate Kafka delivery cannot insert the same award twice.

This is a separate Spring Boot project with its own POM, main class, topics, and port **18085**. Java comments explain why the important lines exist. No other sample application's Java code is needed.

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
java -Xmx256m -jar target/idempotent-loyalty-1.0.0.jar
```

Keep the app running. Do not rebuild or delete its JAR while it is running; stop it with Ctrl+C first. SERVER_PORT overrides the default port. KAFKA_BOOTSTRAP_SERVERS changes the shared broker address except in the security project, which has its own fixed TLS lab configuration.

## Try the feature

```bash
curl -s -X POST localhost:18085/awards \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"award-101","customer":"customer-1","points":10}'
curl -s -X POST localhost:18085/awards \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"award-101","customer":"customer-1","points":10}'
curl -s localhost:18085/points/customer-1
curl -s localhost:18085/results
```

Expected: On a fresh database, points are 10, not 20; results include awarded and duplicate.

All processing is asynchronous. Use returned IDs to match your request to the last 200 in-memory /results entries. Older Kafka records may replay according to group bookmarks. A 202 confirms the write was accepted, not that business work finished.

## Read and explain the code

Suggested order: **LoyaltyPoints.java -> schema.sql -> application.properties**. Application.java starts Spring Boot; DemoLog and ResultController provide a small teaching journal.

Stop and restart the app from the same project directory, then query the same customer: points remain 10. H2 stores rows in ./data/loyalty. One INSERT stores both event identity and points atomically; totals are computed from those durable rows. Only DuplicateKeyException is ignored, not arbitrary database errors. Treat event IDs as immutable: reusing an ID with a different payload is not a valid new award. Never share this H2 file across separate JVMs.

## Check the result manually

Use the curl requests above, then check /results for the returned ID. Compare the observed steps with the expected success and failure behavior. For multiple app instances, inspect the results from each instance.

## Stop and reset

Stop Java with Ctrl+C. Stop project-specific containers using `docker compose down` from this project if it has its own compose.yaml. Stop the shared broker only after any dependent Registry/Connect services: run `docker compose down` from KAFKA-SESSION. Named Docker volumes retain lab data; adding `-v` deliberately erases that Compose project's volumes.

Local H2/Streams files, where used, live under this project's data/ directory. Delete them only with the app stopped and only if you want an intentional reset. These are classroom samples, not production deployments. See ../docs/ADVANCED-PROJECTS.md and ../docs/ADVANCED-VALIDATION.md for the complete map and actual verification results.
