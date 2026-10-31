# Easy-language Word guides and Java comments

Every application project now contains **REAL-LIFE-GUIDE.docx**, explaining a practical business situation, the message flow, failures, Java methods, and how to run the example. There are 15 Word files: one in each of the 14 numbered projects and one in the original combined demo.

See the [Word guide index](docs/WORD-GUIDES.md). Java comments explain why the main methods and Kafka settings are needed. The updated ZIP includes all of these guides.

# Advanced projects 04–14 are ready

All 11 suggested advanced ideas now have separate Spring Boot projects. See the [advanced project map](docs/ADVANCED-PROJECTS.md) for links, ports, setup, and demonstrations, and [validation results](docs/ADVANCED-VALIDATION.md) for what was tested. The root Maven build now includes all 14 projects.

# Three separate Spring Boot + Kafka learning applications

Start with these independent projects. Java comments explain **what each piece does and why it is needed**.

| Project | Port | What it teaches |
|---|---|---|
| [01-pub-sub-application](01-pub-sub-application/README.md) | 18081 | Publish once; email and audit groups both read the message |
| [02-event-driven-application](02-event-driven-application/README.md) | 18082 | Order-created event -> inventory -> stock-reserved event -> shipping |
| [03-dlq-application](03-dlq-application/README.md) | 18083 | Processing failure -> two retries -> dead-letter topic |

Each has its own `pom.xml`, main class, configuration, topics, and README. They can run alone or together against the shared Kafka broker. The event-driven project keeps its logical business handlers in one process for an easy lesson; it does not claim to be three deployed microservices.

From this folder:

```bash
# Start Kafka once for all three applications.
docker compose up -d --wait

# Optional: build all three in one command.
mvn clean package

# Run each command in a separate terminal, or run only the sample you want.
java -jar 01-pub-sub-application/target/pub-sub-application-1.0.0.jar
java -jar 02-event-driven-application/target/event-driven-application-1.0.0.jar
java -jar 03-dlq-application/target/dlq-application-1.0.0.jar

```

Requirements: Java 17+, Maven, Docker Compose v2. See each project's README for curl requests, class reading order, expected output, and stopping instructions. For more projects, see [advanced sample ideas](docs/ADVANCED-SAMPLE-IDEAS.md). Actual verification results for these projects are in [separate-app validation](docs/SEPARATE-APPS-VALIDATION.md).

The PowerPoint and handbook remain available below. The older `demo/` is the original combined example; it is optional and is not needed by the three new applications. Its multi-instance exercise uses port 8081; the new examples use different defaults to avoid a clash.

---

# Kafka session — simple explanations and a working demo

Start with **kafka-training-session.pptx** (48 slides). Every slide has presenter notes.

| File or folder | What it contains |
|---|---|
| `kafka-training-session.pptx` | Editable PowerPoint, plain language, diagrams, embedded notes |
| `preview/kafka-training-session.pdf` | Printable slide preview |
| `docs/SESSION-PLAN.md` | Suggested 60-minute and 100-minute teaching routes |
| `docs/KAFKA-HANDBOOK.md` | Concepts, examples, glossary, and interview questions |
| `docs/PRESENTER-NOTES.md` | Notes and slide content for rehearsal |
| `docs/DEMO-GUIDE.md` | Commands, expected results, scaling, failover, troubleshooting |
| `docs/SOURCES.md` | Official technical references and version choices |
| `docs/VALIDATION.md` | What was actually verified |
| `demo/` | Spring Boot Maven project; Java 17 |
| `compose.yaml` | Single Kafka 4.0.0 broker, KRaft |
| `compose.cluster.yaml` | Three-broker KRaft lab, replication factor 3 |
| `presentation/generate_deck.py` | Rebuilds the PowerPoint and notes |

## Fast start

Requirements: Java 17+, Maven 3.6.3+, Docker Engine with Compose v2, curl. First build needs Internet access for Maven dependencies and Kafka image download. Commands use Bash; curl works in other shells with adjusted quoting.

From this folder (`KAFKA-SESSION`):

```bash
docker compose up -d --wait
cd demo
mvn clean package
java -jar target/kafka-training-demo-1.0.0.jar
```

In another terminal, from `KAFKA-SESSION`:

```bash
curl -i -X POST http://localhost:8080/orders   -H 'Content-Type: application/json'   -d '{"customerId":"customer-1","amount":25.50}'

curl -i -X POST http://localhost:8080/orders   -H 'Content-Type: application/json'   -d '{"customerId":"customer-2","amount":0}'

curl -s http://localhost:8080/demo/status
```

The successful order reaches notifications, inventory, then shipping. The amount-0 order reaches notifications, fails inventory three times, and appears in `orders.DLT`. It does not reach shipping.

```text
                           +--> notifications [notification-service]
POST /orders --> orders ---+
                           +--> inventory [inventory-service]
                                  | success --> inventory.reserved --> shipping
                                  | failure --> retry twice --> orders.DLT --> monitor
```

Brackets show independent consumer group IDs. Kafka stores one source record; groups track separate progress. All listeners are logical services in one JVM for an easy demonstration. HTTP 202 waits for broker acknowledgement but does not wait for the business workflow to finish.

Stop the Java application with Ctrl+C. Stop the lab using `docker compose down`. Data stays in its named volume. `docker compose down -v` erases this lab's retained Kafka data; use only when you want a fresh lab.

## Versions and scope

The teaching sample pins Spring Boot 3.3.5 and Apache Kafka 4.0.0. These are explicit training versions, not a claim of latest releases or a production dependency recommendation. Boot manages Spring Kafka 3.2.4 and the compatible Kafka client. Kafka 4.x has no ZooKeeper mode. The lab uses ordinary consumer groups, three partitions per topic, and non-transactional at-least-once processing.

No database, payment, actual email, or actual stock service is used. Journal entries are in memory, limited to the last 200 entries, and belong to one application instance. Reading the DLQ only logs the event; it does not repair or replay it. Malformed JSON/deserialization recovery is outside the sample; its demonstrated DLQ path covers business processing exceptions.

Rebuild the presentation: `python3 -m pip install -r presentation/requirements.txt`, then `python3 presentation/generate_deck.py`. The PDF is a separately rendered preview; rerender it after changing the deck.
