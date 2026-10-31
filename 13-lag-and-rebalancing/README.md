[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 13. Slow consumers, committed lag, and rebalances

A controlled processing delay lets a burst of records build real consumer lag. The lag endpoint queries Kafka Admin for committed offsets and log ends.

This is a separate Spring Boot project with its own POM, main class, topics, and port **18093**. Java comments explain why the important lines exist. No other sample application's Java code is needed.

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
java -Xmx256m -jar target/lag-and-rebalancing-1.0.0.jar
```

Keep the app running. Do not rebuild or delete its JAR while it is running; stop it with Ctrl+C first. SERVER_PORT overrides the default port. KAFKA_BOOTSTRAP_SERVERS changes the shared broker address except in the security project, which has its own fixed TLS lab configuration.

## Try the feature

```bash
curl -s -X POST 'localhost:18093/work?count=30'
curl -s localhost:18093/lag
curl -s localhost:18093/results
```

Expected: Lag becomes positive after the burst and decreases to zero as the worker catches up.

All processing is asynchronous. Use returned IDs to match your request to the last 200 in-memory /results entries. Older Kafka records may replay according to group bookmarks. A 202 confirms the write was accepted, not that business work finished.

## Read and explain the code

Suggested order: **SlowWorker.java -> work() -> process() -> lag()**. Application.java starts Spring Boot; DemoLog and ResultController provide a small teaching journal.

Start a second instance with SERVER_PORT=18193 to trigger group reassignment; inspect both journals and group membership. Each poll has one record so the deliberate default one-second delay remains below max.poll.interval.ms. Set DEMO_DELAY_MS for experiments, but an excessive value can evict a member and repeat work. Round-robin explicit partitions keep the three lanes busy. Before any commit, the endpoint uses zero as the start position, appropriate for this fresh local topic; arbitrary retained production topics need their actual earliest offsets.

## Check the result manually

Use the curl requests above, then check /results for the returned ID. Compare the observed steps with the expected success and failure behavior. For multiple app instances, inspect the results from each instance.

## Stop and reset

Stop Java with Ctrl+C. Stop project-specific containers using `docker compose down` from this project if it has its own compose.yaml. Stop the shared broker only after any dependent Registry/Connect services: run `docker compose down` from KAFKA-SESSION. Named Docker volumes retain lab data; adding `-v` deliberately erases that Compose project's volumes.

Local H2/Streams files, where used, live under this project's data/ directory. Delete them only with the app stopped and only if you want an intentional reset. These are classroom samples, not production deployments. See ../docs/ADVANCED-PROJECTS.md and ../docs/ADVANCED-VALIDATION.md for the complete map and actual verification results.
