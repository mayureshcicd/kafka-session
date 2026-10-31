[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 08. Transactional consume-transform-produce

The input listener publishes two outputs inside a Kafka transaction. A deliberate failure aborts both output records. Output listeners use read_committed.

This is a separate Spring Boot project with its own POM, main class, topics, and port **18088**. Java comments explain why the important lines exist. No other sample application's Java code is needed.

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
java -Xmx256m -jar target/kafka-transactions-1.0.0.jar
```

Keep the app running. Do not rebuild or delete its JAR while it is running; stop it with Ctrl+C first. SERVER_PORT overrides the default port. KAFKA_BOOTSTRAP_SERVERS changes the shared broker address except in the security project, which has its own fixed TLS lab configuration.

## Try the feature

```bash
curl -s -X POST localhost:18088/process \
  -H 'Content-Type: application/json' \
  -d '{"value":"OK"}'
curl -s -X POST localhost:18088/process \
  -H 'Content-Type: application/json' \
  -d '{"value":"FAIL"}'
curl -s localhost:18088/results
```

Expected: The OK ID has first and second output entries. The FAIL ID eventually has rolled-back and no visible first or second output.

All processing is asynchronous. Use returned IDs to match your request to the last 200 in-memory /results entries. Older Kafka records may replay according to group bookmarks. A 202 confirms the write was accepted, not that business work finished.

## Read and explain the code

Suggested order: **TransactionalFlow.java -> transform() -> rollback() -> application.properties**. Application.java starts Spring Boot; DemoLog and ResultController provide a small teaching journal.

Boot wires the Kafka transaction manager into listener containers. The initial HTTP publish starts its own Kafka transaction. Each app instance gets a unique transaction-ID prefix. The rollback processor retries twice, then deliberately records a skip and advances the failed source offset in a recovery transaction. This skip is explicit teaching behavior, not a production failure policy. Kafka atomicity covers Kafka records and consumed offsets; it does not make an external payment API or database exactly once. read_uncommitted tools can see aborted physical records; read_committed readers do not.

## Check the result manually

Use the curl requests above, then check /results for the returned ID. Compare the observed steps with the expected success and failure behavior. For multiple app instances, inspect the results from each instance.

## Stop and reset

Stop Java with Ctrl+C. Stop project-specific containers using `docker compose down` from this project if it has its own compose.yaml. Stop the shared broker only after any dependent Registry/Connect services: run `docker compose down` from KAFKA-SESSION. Named Docker volumes retain lab data; adding `-v` deliberately erases that Compose project's volumes.

Local H2/Streams files, where used, live under this project's data/ directory. Delete them only with the app stopped and only if you want an intentional reset. These are classroom samples, not production deployments. See ../docs/ADVANCED-PROJECTS.md and ../docs/ADVANCED-VALIDATION.md for the complete map and actual verification results.
