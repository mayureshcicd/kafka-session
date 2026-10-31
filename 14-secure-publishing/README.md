[Word guide: real-life situation and easy code explanation](REAL-LIFE-GUIDE.docx)

# 14. TLS client authentication and Kafka ACLs

A dedicated broker requires client certificates. Both alice and bob authenticate, but only alice has Write permission on the topic.

This is a separate Spring Boot project with its own POM, main class, topics, and port **18094**. Java comments explain why the important lines exist. No other sample application's Java code is needed.

## Start the supporting services

From this project directory (Bash, OpenSSL, and JDK keytool are required):

```bash
bash generate-certificates.sh
docker compose up -d
# Wait until the broker is serving requests, then grant the lab permissions.
bash configure-acls.sh
```

The app must run from this directory so ./certs resolves correctly. You can override DEMO_CERT_DIR with an absolute path. configure-acls.sh can be retried while Kafka finishes startup.

## Build and run

Use Java 17+, Maven, and Docker Compose v2. First build/image download needs Internet. From **this project directory**, in another terminal if necessary:

```bash
mvn clean package
java -Xmx256m -jar target/secure-publishing-1.0.0.jar
```

Keep the app running. Do not rebuild or delete its JAR while it is running; stop it with Ctrl+C first. SERVER_PORT overrides the default port. KAFKA_BOOTSTRAP_SERVERS changes the shared broker address except in the security project, which has its own fixed TLS lab configuration.

## Try the feature

```bash
curl -i -X POST localhost:18094/publish/alice \
  -H 'Content-Type: application/json' \
  -d '{"message":"Allowed writer"}'
curl -i -X POST localhost:18094/publish/bob \
  -H 'Content-Type: application/json' \
  -d '{"message":"Denied writer"}'
curl -s localhost:18094/results
```

Expected: Alice returns 202 and a received entry. Bob returns 403 caused by the Kafka broker ACL, not a Java if-statement rejecting bob.

All processing is asynchronous. Use returned IDs to match your request to the last 200 in-memory /results entries. Older Kafka records may replay according to group bookmarks. A 202 confirms the write was accepted, not that business work finished.

## Read and explain the code

Suggested order: **generate-certificates.sh -> compose.yaml -> configure-acls.sh -> IdentityProducers.java -> SecureOrders.java**. Application.java starts Spring Boot; DemoLog and ResultController provide a small teaching journal.

The application uses two actual producer identities. The admin identity creates topics and consumes results. Bob can Describe the topic so metadata is visible, but cannot Write. TLS hostname verification remains enabled. Lab passwords are intentionally public, certificates expire, and files are generated locally rather than shipped in the ZIP. The controller channel is unexposed plaintext and has an internal superuser: this combined single-node layout is a local teaching shortcut, not a production security configuration. Bind exposed services to loopback and do not publish these generated credentials.

## Check the result manually

Use the curl requests above, then check /results for the returned ID. Compare the observed steps with the expected success and failure behavior. For multiple app instances, inspect the results from each instance.

## Stop and reset

Stop Java with Ctrl+C. Stop project-specific containers using `docker compose down` from this project if it has its own compose.yaml. Stop the shared broker only after any dependent Registry/Connect services: run `docker compose down` from KAFKA-SESSION. Named Docker volumes retain lab data; adding `-v` deliberately erases that Compose project's volumes.

Local H2/Streams files, where used, live under this project's data/ directory. Delete them only with the app stopped and only if you want an intentional reset. These are classroom samples, not production deployments. See ../docs/ADVANCED-PROJECTS.md and ../docs/ADVANCED-VALIDATION.md for the complete map and actual verification results.
