# Word guides for every application project

Each guide has a detailed practical story in easy language, a step-by-step flow, failure and recovery examples, a Java-file walkthrough, run commands, and a small glossary.

| Project | Word guide | Practical situation |
|---|---|---|
| 01-pub-sub-application | [REAL-LIFE-GUIDE.docx](../01-pub-sub-application/REAL-LIFE-GUIDE.docx) | An online shop tells email and reporting about the same order. |
| 02-event-driven-application | [REAL-LIFE-GUIDE.docx](../02-event-driven-application/REAL-LIFE-GUIDE.docx) | An order leads to stock reservation, which then leads to shipping. |
| 03-dlq-application | [REAL-LIFE-GUIDE.docx](../03-dlq-application/REAL-LIFE-GUIDE.docx) | A payment-related message fails, is retried, and is saved for investigation. |
| 04-ordering-and-groups | [REAL-LIFE-GUIDE.docx](../04-ordering-and-groups/REAL-LIFE-GUIDE.docx) | An order moves from CREATED to PAID without mixing its updates across lanes. |
| 05-idempotent-loyalty | [REAL-LIFE-GUIDE.docx](../05-idempotent-loyalty/REAL-LIFE-GUIDE.docx) | A customer gets 10 points for one purchase, not 20 after redelivery. |
| 06-non-blocking-retry | [REAL-LIFE-GUIDE.docx](../06-non-blocking-retry/REAL-LIFE-GUIDE.docx) | A temporary email problem should not stop every later notification. |
| 07-streams-dashboard | [REAL-LIFE-GUIDE.docx](../07-streams-dashboard/REAL-LIFE-GUIDE.docx) | A shop dashboard shows orders per customer in each minute. |
| 08-kafka-transactions | [REAL-LIFE-GUIDE.docx](../08-kafka-transactions/REAL-LIFE-GUIDE.docx) | A processing step must publish both its Kafka results, or neither. |
| 09-outbox-cdc | [REAL-LIFE-GUIDE.docx](../09-outbox-cdc/REAL-LIFE-GUIDE.docx) | A database transaction saves an order and a publishable fact together. |
| 10-schema-evolution | [REAL-LIFE-GUIDE.docx](../10-schema-evolution/REAL-LIFE-GUIDE.docx) | Add a field safely while older event versions still exist. |
| 11-compaction-and-tombstones | [REAL-LIFE-GUIDE.docx](../11-compaction-and-tombstones/REAL-LIFE-GUIDE.docx) | Keep current customer names and represent deletions clearly. |
| 12-broker-failover | [REAL-LIFE-GUIDE.docx](../12-broker-failover/REAL-LIFE-GUIDE.docx) | An ordering system needs safe writes while one server is being repaired. |
| 13-lag-and-rebalancing | [REAL-LIFE-GUIDE.docx](../13-lag-and-rebalancing/REAL-LIFE-GUIDE.docx) | A burst of background jobs arrives faster than one worker can finish. |
| 14-secure-publishing | [REAL-LIFE-GUIDE.docx](../14-secure-publishing/REAL-LIFE-GUIDE.docx) | Two applications identify themselves, but only the approved writer can send orders. |
| demo | [REAL-LIFE-GUIDE.docx](../demo/REAL-LIFE-GUIDE.docx) | An order reaches notifications, inventory, shipping, and a visible failure path. |

There are 14 numbered sample projects plus the original combined demo: 15 Word files.


The Word guides are standalone files and can be edited directly in Word.
