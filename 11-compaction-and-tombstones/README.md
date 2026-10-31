[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 11. Compacted state and tombstones

Publish latest customer values by key, then rebuild state from the retained log. A real null Kafka value represents a deletion.

This is a separate Spring Boot project with its own POM, main class, topics, and port **18091**. Java comments explain why the important lines exist. No other sample application's Java code is needed.

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
java -Xmx256m -jar target/compaction-and-tombstones-1.0.0.jar
```

Keep the app running. Do not rebuild or delete its JAR while it is running; stop it with Ctrl+C first. SERVER_PORT overrides the default port. KAFKA_BOOTSTRAP_SERVERS changes the shared broker address except in the security project, which has its own fixed TLS lab configuration.

## Try the feature

```bash
curl -s -X POST localhost:18091/customers/customer-1 \
  -H 'Content-Type: application/json' \
  -d '{"name":"Old name"}'
curl -s -X POST localhost:18091/customers/customer-1 \
  -H 'Content-Type: application/json' \
  -d '{"name":"New name"}'
curl -s localhost:18091/customers
curl -s localhost:18091/rebuild
curl -s -X DELETE localhost:18091/customers/customer-1
curl -s localhost:18091/rebuild
```

Expected: State is New name after the update; the key disappears after the tombstone is consumed. A separate replay produces the same latest state.

All processing is asynchronous. Use returned IDs to match your request to the last 200 in-memory /results entries. Older Kafka records may replay according to group bookmarks. A 202 confirms the write was accepted, not that business work finished.

## Read and explain the code

Suggested order: **CustomerState.java -> delete() -> rebuild() -> Topics.java**. Application.java starts Spring Boot; DemoLog and ResultController provide a small teaching journal.

Compaction is asynchronous: the verifier checks state reconstruction, not instant physical removal of old records. delete.retention.ms is set to one day for tombstones in this lab. The live listener uses a fresh group on startup so it can rebuild its in-memory view. The /rebuild endpoint uses manual partition assignment and captures an end offset to avoid disturbing the live group. This small example uses one partition; scale it with one bookmark per partition. Snapshot endpoints need not be identical while new writes are in flight.

## Check the result manually

Use the curl requests above, then check /results for the returned ID. Compare the observed steps with the expected success and failure behavior. For multiple app instances, inspect the results from each instance.

## Stop and reset

Stop Java with Ctrl+C. Stop project-specific containers using `docker compose down` from this project if it has its own compose.yaml. Stop the shared broker only after any dependent Registry/Connect services: run `docker compose down` from KAFKA-SESSION. Named Docker volumes retain lab data; adding `-v` deliberately erases that Compose project's volumes.

Local H2/Streams files, where used, live under this project's data/ directory. Delete them only with the app stopped and only if you want an intentional reset. These are classroom samples, not production deployments. See ../docs/ADVANCED-PROJECTS.md and ../docs/ADVANCED-VALIDATION.md for the complete map and actual verification results.
