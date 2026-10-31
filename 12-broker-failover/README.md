[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 12. Replication, ISR, and leader failover

A separate three-broker KRaft cluster stores RF3 topics with min ISR2. The producer waits for all in-sync replicas.

This is a separate Spring Boot project with its own POM, main class, topics, and port **18092**. Java comments explain why the important lines exist. No other sample application's Java code is needed.

## Start the supporting services

From this project directory:

```bash
docker compose up \
  -d --wait
```

This cluster uses its own brokers, not the shared single broker.

## Build and run

Use Java 17+, Maven, and Docker Compose v2. First build/image download needs Internet. From **this project directory**, in another terminal if necessary:

```bash
mvn clean package
java -Xmx256m -jar target/broker-failover-1.0.0.jar
```

Keep the app running. Do not rebuild or delete its JAR while it is running; stop it with Ctrl+C first. SERVER_PORT overrides the default port. KAFKA_BOOTSTRAP_SERVERS changes the shared broker address except in the security project, which has its own fixed TLS lab configuration.

## Try the feature

```bash
curl -s -X POST localhost:18092/messages \
  -H 'Content-Type: application/json' \
  -d '{"message":"Before broker failure"}'
docker compose stop kafka1
curl -s -X POST localhost:18092/messages \
  -H 'Content-Type: application/json' \
  -d '{"message":"After broker failure"}'
curl -s localhost:18092/results
docker compose start kafka1
```

Expected: Both IDs are eventually received when the remaining pair is healthy and has controller majority and sufficient ISR.

All processing is asynchronous. Use returned IDs to match your request to the last 200 in-memory /results entries. Older Kafka records may replay according to group bookmarks. A 202 confirms the write was accepted, not that business work finished.

## Read and explain the code

Suggested order: **FailoverMessages.java -> Topics.java -> compose.yaml**. Application.java starts Spring Boot; DemoLog and ResultController provide a small teaching journal.

This project has its own cluster, project name, ports 29192–29194, and volumes. It does not reuse the root cluster. One broker loss is tested; losing two can remove controller majority/minimum ISR and stop writes. Three containers on one laptop are not three independent physical failure domains. Combined broker/controller roles are a teaching shortcut. Allow election and replica catch-up time.

## Check the result manually

Use the curl requests above, then check /results for the returned ID. Compare the observed steps with the expected success and failure behavior. For multiple app instances, inspect the results from each instance.

## Stop and reset

Stop Java with Ctrl+C. Stop project-specific containers using `docker compose down` from this project if it has its own compose.yaml. Stop the shared broker only after any dependent Registry/Connect services: run `docker compose down` from KAFKA-SESSION. Named Docker volumes retain lab data; adding `-v` deliberately erases that Compose project's volumes.

Local H2/Streams files, where used, live under this project's data/ directory. Delete them only with the app stopped and only if you want an intentional reset. These are classroom samples, not production deployments. See ../docs/ADVANCED-PROJECTS.md and ../docs/ADVANCED-VALIDATION.md for the complete map and actual verification results.

## Inspect replicas and group assignments

```bash
docker compose exec kafka2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka2:19092 --describe --topic sample.failover.messages
```

Before failure, expect three ISR IDs. After one broker stops, remaining partition leaders should be on the healthy pair. After restarting it, wait for ISR to return to three.
