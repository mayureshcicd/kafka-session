[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 06. Retry topics and delayed recovery

Spring routes a failed record through retry topics. The main partition can process a later FAST record while the earlier RETRY record waits.

This is a separate Spring Boot project with its own POM, main class, topics, and port **18086**. Java comments explain why the important lines exist. No other sample application's Java code is needed.

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
java -Xmx256m -jar target/non-blocking-retry-1.0.0.jar
```

Keep the app running. Do not rebuild or delete its JAR while it is running; stop it with Ctrl+C first. SERVER_PORT overrides the default port. KAFKA_BOOTSTRAP_SERVERS changes the shared broker address except in the security project, which has its own fixed TLS lab configuration.

## Try the feature

```bash
curl -s -X POST localhost:18086/emails \
  -H 'Content-Type: application/json' \
  -d '{"message":"RETRY"}'
curl -s -X POST localhost:18086/emails \
  -H 'Content-Type: application/json' \
  -d '{"message":"FAST"}'
curl -s -X POST localhost:18086/emails \
  -H 'Content-Type: application/json' \
  -d '{"message":"FAIL"}'
curl -s localhost:18086/results
```

Expected: FAST completes first. RETRY succeeds on attempt 3, after two roughly three-second delays. FAIL reaches a DLT after three attempts.

All processing is asynchronous. Use returned IDs to match your request to the last 200 in-memory /results entries. Older Kafka records may replay according to group bookmarks. A 202 confirms the write was accepted, not that business work finished.

## Read and explain the code

Suggested order: **EmailRetries.java, especially @RetryableTopic and @DltHandler**. Application.java starts Spring Boot; DemoLog and ResultController provide a small teaching journal.

The main topic deliberately has one partition, so the fast message is not merely using another lane. Retry-topic ordering differs from blocking retries. The flaky email provider is simulated using in-memory attempt counters; a restart resets that simulation. Use the ordinary non-transactional containers here: non-blocking retries are not combined with container transactions. Spring automatically creates the retry/DLT infrastructure.

## Check the result manually

Use the curl requests above, then check /results for the returned ID. Compare the observed steps with the expected success and failure behavior. For multiple app instances, inspect the results from each instance.

## Stop and reset

Stop Java with Ctrl+C. Stop project-specific containers using `docker compose down` from this project if it has its own compose.yaml. Stop the shared broker only after any dependent Registry/Connect services: run `docker compose down` from KAFKA-SESSION. Named Docker volumes retain lab data; adding `-v` deliberately erases that Compose project's volumes.

Local H2/Streams files, where used, live under this project's data/ directory. Delete them only with the app stopped and only if you want an intentional reset. These are classroom samples, not production deployments. See ../docs/ADVANCED-PROJECTS.md and ../docs/ADVANCED-VALIDATION.md for the complete map and actual verification results.
