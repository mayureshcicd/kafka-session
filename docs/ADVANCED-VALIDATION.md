# Advanced projects — observed verification results

Verified on 31 October 2026 using Java 17, Spring Boot 3.3.5 / Spring Kafka 3.2.4, Apache Kafka 4.0.0, and the additional services pinned in each project's Compose file.

| Project | Actual live verification | Result |
|---|---|---|
| 04 Ordering/groups | CREATED and PAID for the same order used the same partition and increasing offsets; listener saw them in order | Passed |
| 05 Idempotency | Published one award twice; database total increased once; duplicate recorded | Passed |
| 05 Persistence | Stopped/restarted its JVM; the previously awarded customer still had 10 points | Passed |
| 06 Retry topics | FAST completed ahead of RETRY on a single-partition input; RETRY succeeded on attempt 3; permanent FAIL reached DLT | Passed |
| 07 Streams | Real materialized window count reached 2; stream time advanced; a late record for the closed window did not increase the old count | Passed |
| 08 Transactions | Committed input produced both visible outputs; failing input rolled back and produced neither output for read_committed listeners | Passed |
| 09 Outbox/database | Deliberate failure after two INSERTs rolled back both; successful DB commit produced a real Debezium CDC event | Passed |
| 09 CDC recovery | Stopped Connect, saved an order, confirmed its CDC result was absent, restarted Connect, then observed that exact event | Passed |
| 10 Schema/Avro | Registry accepted v1/v2, rejected incompatible schema with 409, and both real Avro payload versions were decoded | Passed |
| 11 Compaction/state | Latest update reconstructed correctly; null tombstone removed the key; separate replay rebuilt the same state | Passed |
| 12 Replication | Dedicated topic showed 3 partitions, RF3, min ISR2, and all three replicas initially in ISR | Passed |
| 12 Failover | Stopped kafka1 in its dedicated cluster; HTTP publish and Kafka consumption still succeeded; restarted kafka1 afterward | Passed |
| 13 Lag | Burst created positive committed lag; worker caught up to zero | Passed |
| 13 Rebalance | Started second instance; actual group assignment showed two members owning 2 and 1 partitions | Passed |
| 14 TLS/ACL | Certificate-authenticated alice published and was consumed; certificate-authenticated bob was rejected with broker authorization failure (HTTP 403) | Passed |

These results were recorded using live integration checks plus explicit restart/failure commands before the optional helper scripts were removed. They are live integration exercises, not a claim of mocked/unit-test coverage. The root Maven aggregator includes projects 01–14. Each new project has its own POM, application entry point, configuration, commented Java source, and README.

## Important limits

- These are local classroom examples with simulated business actions. They are not production deployment templates.
- The Streams exercise checks a real state store and late-window behavior. It does not measure production throughput. Store retention is explicitly one day; the ten-second grace controls accepting late records, not retention.
- The compaction check verifies logical latest-state reconstruction. It does not assert the background log cleaner immediately removed physical older records.
- H2 persistence was checked across one app restart. It is not an external multi-node database or a multi-JVM shared storage setup.
- Kafka transactions demonstrate the Kafka record/offset boundary. They do not include arbitrary external database or payment side effects.
- CDC recovery depends on retained PostgreSQL WAL/slot and Kafka Connect offsets. Long outages, WAL loss, or volume deletion need a recovery plan. CDC delivery can repeat.
- Broker failover was checked with one node stopped on a single physical host; it does not validate host/zone/region failure.
- Security checks exercise real mTLS and Kafka ACL enforcement. Public lab passwords, single-node combined roles, and the unexposed plaintext controller channel are teaching shortcuts. Certificates and keys must be generated locally and are excluded from the archive.
- Negative checks observe the system after the relevant recovery signal; they do not claim mathematical proof that no future delivery is possible under every failure scenario.

Temporary sample JVMs and containers are stopped after validation. Local lab volumes and data files remain for reruns and are excluded from the ZIP. The previously existing service on port 8081 is not part of these examples and was not modified.
