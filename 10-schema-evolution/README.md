[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 10. Avro and Schema Registry compatibility

Register a v1 schema, add an optional field in v2, and try an incompatible amount type. Publish real Avro binary records containing the registered schema ID.

This is a separate Spring Boot project with its own POM, main class, topics, and port **18090**. Java comments explain why the important lines exist. No other sample application's Java code is needed.

## Start the supporting services

First, from `KAFKA-SESSION`, start the shared Kafka broker:

```bash
docker compose up \
  -d --wait
```

Then from this project directory, start Schema Registry:

```bash
docker compose up -d
curl localhost:28081/subjects
```

Wait for the Registry endpoint before sending schema requests.


## Build and run

Use Java 17+, Maven, and Docker Compose v2. First build/image download needs Internet. From **this project directory**, in another terminal if necessary:

```bash
mvn clean package
java -Xmx256m -jar target/schema-evolution-1.0.0.jar
```

Keep the app running. Do not rebuild or delete its JAR while it is running; stop it with Ctrl+C first. SERVER_PORT overrides the default port. KAFKA_BOOTSTRAP_SERVERS changes the shared broker address except in the security project, which has its own fixed TLS lab configuration.

## Try the feature

```bash
curl -i -X POST localhost:18090/schemas/v1
curl -i -X POST localhost:18090/schemas/v2
curl -i -X POST localhost:18090/schemas/bad
curl -s -X POST localhost:18090/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"schema-order-1","amount":10,"version":2,"note":"Optional field"}'
curl -s localhost:18090/results
```

Expected: v1 and v2 return 200. bad returns the Registry 409 incompatibility response. The order is decoded using its stored writer schema.

All processing is asynchronous. Use returned IDs to match your request to the last 200 in-memory /results entries. Older Kafka records may replay according to group bookmarks. A 202 confirms the write was accepted, not that business work finished.

## Read and explain the code

Suggested order: **schemas/*.avsc -> RegistryClient.java -> AvroOrders.java**. Application.java starts Spring Boot; DemoLog and ResultController provide a small teaching journal.

The subject uses FULL_TRANSITIVE compatibility. The v2 note field is nullable with a default so it is compatible in both reading directions. Framing is magic byte 0 + four-byte schema ID + Avro binary payload, shown explicitly rather than hidden behind a serializer library. The consumer fetches each writer schema by ID. Broker-side validation is not enabled: a bare Kafka broker does not reject arbitrary bytes based on these schemas. This demo checks registration and decodes v1/v2; it does not pretend to be a complete schema client with caches, authentication, or bounded retries.

## Check the result manually

Use the curl requests above, then check /results for the returned ID. Compare the observed steps with the expected success and failure behavior. For multiple app instances, inspect the results from each instance.

## Stop and reset

Stop Java with Ctrl+C. Stop project-specific containers using `docker compose down` from this project if it has its own compose.yaml. Stop the shared broker only after any dependent Registry/Connect services: run `docker compose down` from KAFKA-SESSION. Named Docker volumes retain lab data; adding `-v` deliberately erases that Compose project's volumes.

Local H2/Streams files, where used, live under this project's data/ directory. Delete them only with the app stopped and only if you want an intentional reset. These are classroom samples, not production deployments. See ../docs/ADVANCED-PROJECTS.md and ../docs/ADVANCED-VALIDATION.md for the complete map and actual verification results.
