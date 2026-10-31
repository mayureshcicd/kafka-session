# Presenter notes

Use the slides with docs/SESSION-PLAN.md and docs/DEMO-GUIDE.md. These notes are also embedded in the PowerPoint.

## 01. Apache Kafka

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. A simple guide to events, pub/sub, clusters,
and handling failures Includes a small Spring Boot demonstration

Slide content:

- A simple guide to events, pub/sub, clusters,; and handling failures
- Includes a small Spring Boot demonstration

## 02. Today's learning path

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. 1. Why Kafka? - The problem it solves
- Benefits and trade-offs
- Simple real-life example 2. How it works - Topics, partitions, brokers
- Producers, consumers, groups
- Offsets and replication 3. Use it well - Event-driven design
- Single broker vs cluster
- Retries, DLQ, and advanced tools 4. See it work - Spring Boot publisher
- Two subscriber services
- Failure sent to a DLQ

Slide content:

- 1. Why Kafka?
- - The problem it solves; - Benefits and trade-offs; - Simple real-life example
- 2. How it works
- - Topics, partitions, brokers; - Producers, consumers, groups; - Offsets and replication
- 3. Use it well
- - Event-driven design; - Single broker vs cluster; - Retries, DLQ, and advanced tools
- 4. See it work
- - Spring Boot publisher; - Two subscriber services; - Failure sent to a DLQ

## 03. An event is a fact that already happened

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. An everyday example - A customer placed an order.
- That fact is an event: OrderCreated.
- Email, stock, and reports can react later. A useful event has - A unique event or order ID.
- A business key, such as orderId.
- A timestamp and schema version.
- Only the data consumers need. Event vs command - Event: OrderCreated tells us what happened.
- Command: ReserveStock asks for an action.
- Kafka can carry both; agree on meaning.

Slide content:

- An everyday example
- - A customer placed an order.; - That fact is an event: OrderCreated.; - Email, stock, and reports can react later.
- A useful event has
- - A unique event or order ID.; - A business key, such as orderId.; - A timestamp and schema version.; - Only the data consumers need.
- Event vs command
- - Event: OrderCreated tells us what happened.; - Command: ReserveStock asks for an action.; - Kafka can carry both; agree on meaning.

## 04. Why do we need Kafka?

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. er arrives once.
- Inventory, billing, and email all need to react.
- Direct calls make the order service know every destination. Systems are not always ready - A receiving service may be slow or offline.
- A direct call can fail the whole request.
- A queue can hold the work until it is read. We may need the history - A new service may need old events.
- A bug fix may require replaying events.
- Kafka keeps records for a configured time or size.

Slide content:

- Many systems need the same fact
- - An order arrives once.; - Inventory, billing, and email all need to react.; - Direct calls make the order service know every destination.
- Systems are not always ready
- - A receiving service may be slow or offline.; - A direct call can fail the whole request.; - A queue can hold the work until it is read.
- We may need the history
- - A new service may need old events.; - A bug fix may require replaying events.; - Kafka keeps records for a configured time or size.

## 05. What is Kafka?

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. ink of a shared event journal - A producer writes a record.
- Kafka stores records in a topic.
- Consumers read records when they are ready. Kafka is a distributed system - Data is split across partitions.
- Partitions can be copied to other brokers.
- A group of brokers forms a Kafka cluster. Not just a queue - Records can remain after they are read.
- Different consumer groups can read independently.
- Consumers can often rewind and read again.

Slide content:

- Think of a shared event journal
- - A producer writes a record.; - Kafka stores records in a topic.; - Consumers read records when they are ready.
- Kafka is a distributed system
- - Data is split across partitions.; - Partitions can be copied to other brokers.; - A group of brokers forms a Kafka cluster.
- Not just a queue
- - Records can remain after they are read.; - Different consumer groups can read independently.; - Consumers can often rewind and read again.

## 06. What are the benefits of Kafka?

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. rotect data from a broker failure.
- Consumers can catch up after being offline.
- Producer and consumer failures can be handled separately. Reuse and replay - One event can serve many independent teams.
- New consumers can start from retained history.
- Useful for analytics and integrations. Remember the cost - Kafka needs operations and monitoring.
- It adds eventual, not instant, processing.
- Do not use it just to make a tiny app fashionable.

Slide content:

- Scale
- - Partitions let work run in parallel.; - Add consumers in a group to share work.; - Add brokers to spread storage and traffic.
- Resilience
- - Replication can protect data from a broker failure.; - Consumers can catch up after being offline.; - Producer and consumer failures can be handled separately.
- Reuse and replay
- - One event can serve many independent teams.; - New consumers can start from retained history.; - Useful for analytics and integrations.
- Remember the cost
- - Kafka needs operations and monitoring.; - It adds eventual, not instant, processing.; - Do not use it just to make a tiny app fashionable.

## 07. When should we use Kafka?

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. A good fit - Many services need the same events.
- Traffic arrives in bursts.
- Readers need replay or independent progress.
- Analytics and real-time pipelines. A simpler option may fit - Need an immediate answer? Consider HTTP.
- A tiny background task may only need a queue.
- Kafka adds storage, monitoring, and operating work.

Slide content:

- A good fit
- - Many services need the same events.; - Traffic arrives in bursts.; - Readers need replay or independent progress.; - Analytics and real-time pipelines.
- A simpler option may fit
- - Need an immediate answer? Consider HTTP.; - A tiny background task may only need a queue.; - Kafka adds storage, monitoring, and operating work.

## 08. Words you will hear

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. laces - Producer: writes records.
- Consumer: reads records.
- Broker: one Kafka server.
- Cluster: brokers working together. Data organization - Topic: named stream, like orders.
- Partition: one ordered piece of a topic.
- Record: key, value, time, and headers.
- Offset: record position in a partition. Reading together - Consumer group: consumers sharing work.
- Each group tracks its own offsets.
- Different groups can each see the full stream.

Slide content:

- People and places
- - Producer: writes records.; - Consumer: reads records.; - Broker: one Kafka server.; - Cluster: brokers working together.
- Data organization
- - Topic: named stream, like orders.; - Partition: one ordered piece of a topic.; - Record: key, value, time, and headers.; - Offset: record position in a partition.
- Reading together
- - Consumer group: consumers sharing work.; - Each group tracks its own offsets.; - Different groups can each see the full stream.

## 09. Pub/sub: publish once, many can listen

Point to the two branches. Neither service forwards the order to the other. Kafka stores one record and gives each group independent progress. Each group can reread retained records.

Slide content:

- Order API
- Publishes one order
- orders topic
- One stored record
- Notifications
- Own consumer group
- Inventory
- Own consumer group
- Both groups read the same event.; ; Inventory publishes a new event. Shipping listens to it.

## 10. Consumer groups: copies or shared work?

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. s - Each group gets its own view of the topic.
- Email and inventory both see each order.
- Each group commits its own offsets. Same group ID - Members share the partitions.
- One partition is assigned to one member at a time.
- Each record is handled by one member in that group. Important limit - For standard consumer groups: active readers are limited by partitions.
- Extra consumers may sit idle.
- More partitions can allow more parallel work.

Slide content:

- Different group IDs
- - Each group gets its own view of the topic.; - Email and inventory both see each order.; - Each group commits its own offsets.
- Same group ID
- - Members share the partitions.; - One partition is assigned to one member at a time.; - Each record is handled by one member in that group.
- Important limit
- - For standard consumer groups: active readers are limited by partitions.; - Extra consumers may sit idle.; - More partitions can allow more parallel work.

## 11. How Kafka stores data inside

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Partition 0 Separate ordered log 0 1 2 3 4 Partition 1 Separate ordered log 0 1 2 3 4 Partition 2 Separate ordered log 0 1 2 3 4 Numbers are offsets. New records append on the right.

Slide content:

- Partition 0
- Separate ordered log
- 0
- 1
- 2
- 3
- 4
- Partition 1
- Separate ordered log
- 0
- 1
- 2
- 3
- 4
- Partition 2
- Separate ordered log
- 0
- 1
- 2
- 3
- 4
- Numbers are offsets. New records append on the right.

## 12. What happens when a producer sends?

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. 1. Build a record - Choose a topic.
- Set a value and optional key.
- Add headers if useful. 2. Pick a partition - A key usually sends related records to the same partition.
- Without a key, the producer can spread records.
- The partition determines the ordering lane. 3. Send and confirm - Batching and compression can improve efficiency.
- Kafka acknowledges according to the chosen setting.
- The producer can retry temporary errors.

Slide content:

- 1. Build a record
- - Choose a topic.; - Set a value and optional key.; - Add headers if useful.
- 2. Pick a partition
- - A key usually sends related records to the same partition.; - Without a key, the producer can spread records.; - The partition determines the ordering lane.
- 3. Send and confirm
- - Batching and compression can improve efficiency.; - Kafka acknowledges according to the chosen setting.; - The producer can retry temporary errors.

## 13. How records reach disk efficiently

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Producer - Buffers records and sends batches.
- Compression makes batches smaller.
- Metadata tells the client which broker leads. Broker - Appends batches to segment files.
- Uses the operating system page cache.
- Indexes help locate offsets and times. Consumer - Pulls batches with poll().
- Reads from partition leaders by default.
- Reading does not remove a record.

Slide content:

- Producer
- - Buffers records and sends batches.; - Compression makes batches smaller.; - Metadata tells the client which broker leads.
- Broker
- - Appends batches to segment files.; - Uses the operating system page cache.; - Indexes help locate offsets and times.
- Consumer
- - Pulls batches with poll().; - Reads from partition leaders by default.; - Reading does not remove a record.

## 14. What happens when a consumer reads?

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Read - A consumer joins a group.
- The group coordinates partition assignments.
- The consumer polls records. Process - The application handles each record.
- It may call a database or another service.
- Slow work can delay later records in that partition. Save progress - The group stores an offset.
- On restart, it continues from its committed position.
- Commit only after the work you need is complete.

Slide content:

- Read
- - A consumer joins a group.; - The group coordinates partition assignments.; - The consumer polls records.
- Process
- - The application handles each record.; - It may call a database or another service.; - Slow work can delay later records in that partition.
- Save progress
- - The group stores an offset.; - On restart, it continues from its committed position.; - Commit only after the work you need is complete.

## 15. Offsets: a bookmark for each partition

Draw offsets 0 through 4. After processing 4, write committed offset 5. Ask: is offset 5 a message ID across the whole topic? No. Mention offsets can have gaps after compaction or transactions.

Slide content:

- A record position
- - Partition 0: offsets 0, 1, 2, 3, 4.; - Partition 1 has its own 0, 1, 2...; - Offset 4 in P0 is unrelated to offset 4 in P1.
- Committed offset
- - Processed P0 offsets 0 to 4? Commit 5.; - 5 is the next record the group expects to read.; - Each group has separate bookmarks.
- After a restart
- - Resume from the committed offset.; - Uncommitted work can repeat.; - Kafka stores group bookmarks in __consumer_offsets.

## 16. Partitions: parallelism and ordering

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. n.
- It does not promise one total order across a topic.
- Use the same key for records that must stay together. Example key - Key = customerId.
- All events for one customer go to one partition.
- Different customers can be processed in parallel. Choose carefully - Partition count affects parallelism.
- Too few can limit throughput.
- Adding partitions later can change key-to-partition mapping.
- Membership changes can trigger a group rebalance.

Slide content:

- Ordering
- - Kafka preserves order inside one partition.; - It does not promise one total order across a topic.; - Use the same key for records that must stay together.
- Example key
- - Key = customerId.; - All events for one customer go to one partition.; - Different customers can be processed in parallel.
- Choose carefully
- - Partition count affects parallelism.; - Too few can limit throughput.; - Adding partitions later can change key-to-partition mapping.; - Membership changes can trigger a group rebalance.

## 17. Lag: how far is a reader behind?

The simplified lag calculation uses the log end and committed positions. Transaction visibility, retention, and compaction complicate interpreting the number as a precise count of business events. Monitor event age and processing success too.

Slide content:

- Simple example
- - Log end offset = 100.; - Group committed offset = 90.; - Committed lag = 100 - 90 = 10 positions.
- Why it grows
- - Consumers are slow or stopped.; - A bad record keeps failing.; - Traffic grew, or one partition is overloaded.
- How to respond
- - Check processing time and errors.; - Add consumers up to the partition limit.; - Watch event age as well as lag count.

## 18. Rebalancing and multiple app instances

The partition limit describes standard consumer groups used in this demo. Newer Kafka versions also have share-group features with different semantics; they are version-dependent and outside this lab. Our listeners each have concurrency 1. Two app instances mean two members in each group.

Slide content:

- Three partitions, one app
- - One member owns P0, P1, and P2.; - It can process all records.; - Partitions stay separate ordering lanes.
- Three partitions, two apps
- - Same group ID: partitions are shared.; - One app may get two; the other gets one.; - A fourth member would be idle with three partitions.
- When members change
- - Joining or leaving can rebalance ownership.; - Processing may pause during reassignment.; - Do not exceed max.poll.interval.ms with slow work.

## 19. Kafka with one broker

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Good for learning - One Kafka server runs on one machine.
- Simple to start with Docker or locally.
- A Spring Boot demo can use one broker. What still works - Topics, partitions, producers, consumers.
- Consumer groups and offsets.
- Retries and a DLQ can be demonstrated. What it cannot do - No broker redundancy.
- A broker or machine failure stops service.
- Replication factor 1 is not fault tolerant.

Slide content:

- Good for learning
- - One Kafka server runs on one machine.; - Simple to start with Docker or locally.; - A Spring Boot demo can use one broker.
- What still works
- - Topics, partitions, producers, consumers.; - Consumer groups and offsets.; - Retries and a DLQ can be demonstrated.
- What it cannot do
- - No broker redundancy.; - A broker or machine failure stops service.; - Replication factor 1 is not fault tolerant.

## 20. Kafka with multiple brokers

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Broker 1 P0: LEADER P1: follower copy P2: follower copy Broker 2 P0: follower copy P1: LEADER P2: follower copy Broker 3 P0: follower copy P1: follower copy P2: LEADER One leader per partition. Three copies per partition. Leaders can be spread across brokers.

Slide content:

- Broker 1
- P0: LEADER
- P1: follower copy
- P2: follower copy
- Broker 2
- P0: follower copy
- P1: LEADER
- P2: follower copy
- Broker 3
- P0: follower copy
- P1: follower copy
- P2: LEADER
- One leader per partition. Three copies per partition. Leaders can be spread across brokers.

## 21. One broker vs three brokers

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. One local broker - Replication factor = 1.
- Easy to learn and run.
- If it stops, there is no other data replica. Three-broker lab - Replication factor = 3.
- Minimum in-sync replicas = 2.
- A healthy remaining pair can keep writing. Real deployment - Spread replicas across hosts or zones.
- Use a controller quorum, commonly 3 or 5.
- Combined broker/controllers here are a lab shortcut.

Slide content:

- One local broker
- - Replication factor = 1.; - Easy to learn and run.; - If it stops, there is no other data replica.
- Three-broker lab
- - Replication factor = 3.; - Minimum in-sync replicas = 2.; - A healthy remaining pair can keep writing.
- Real deployment
- - Spread replicas across hosts or zones.; - Use a controller quorum, commonly 3 or 5.; - Combined broker/controllers here are a lab shortcut.

## 22. ZooKeeper and KRaft: old and new

Correct the common spelling: KRaft, pronounced craft. Separate the metadata controller leader from data partition leaders. Three combined-role containers are convenient for a lab; production commonly separates dedicated controllers.

Slide content:

- Older installations
- - ZooKeeper was a separate coordination system.; - It helped manage metadata and controllers.; - You may still find it in older tutorials.
- Modern Kafka
- - KRaft means Kafka uses Raft for metadata.; - Controllers keep a replicated metadata log.; - Kafka 4.0 removed ZooKeeper support.
- Controller vs partition leader
- - Controllers manage cluster metadata.; - Partition leaders handle reads and writes for data.; - They are different responsibilities.

## 23. Replication, acks, and durability

acks=all waits for the current ISR, not just a fixed number equal to min ISR. min.insync.replicas sets the minimum ISR size that allows these writes. With RF3/minISR2, one healthy-broker failure is tolerated. This is not per-record fsync or a guarantee against every correlated failure.

Slide content:

- Replication factor
- - Number of stored copies for a partition.; - Often 3 in production, if the cluster supports it.; - Copies should be on different brokers.
- Producer acks
- - acks=0: do not wait for broker confirmation.; - acks=1: wait for the leader.; - acks=all: wait for all current in-sync replicas.
- ISR and min ISR
- - ISR means replicas caught up enough to count.; - min.insync.replicas can reject risky writes.; - The right settings balance availability and safety.

## 24. Leader failure: what happens next?

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. fore a failure - P0 leader is on broker 1.
- Brokers 2 and 3 hold follower replicas.
- Followers fetch and copy the leader log. Broker 1 stops - The controller selects an eligible in-sync replica.
- Clients refresh metadata and retry.
- A brief delay is normal during recovery. Safety settings matter - RF=3, min ISR=2, acks=all tolerates one broker loss.
- With only one ISR left, writes are rejected.
- A metadata quorum must also remain available.

Slide content:

- Before a failure
- - P0 leader is on broker 1.; - Brokers 2 and 3 hold follower replicas.; - Followers fetch and copy the leader log.
- Broker 1 stops
- - The controller selects an eligible in-sync replica.; - Clients refresh metadata and retry.; - A brief delay is normal during recovery.
- Safety settings matter
- - RF=3, min ISR=2, acks=all tolerates one broker loss.; - With only one ISR left, writes are rejected.; - A metadata quorum must also remain available.

## 25. Producer safety and speed settings

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. acks=all plus min ISR protects acknowledged writes.
- enable.idempotence avoids retry duplicates in Kafka.
- Check send results; do not ignore async errors. Speed - batch.size controls batch size limits.
- linger.ms allows time to build batches.
- compression.type reduces network and disk use. Limits - delivery.timeout.ms bounds delivery waiting.
- Idempotent producer does not deduplicate business orders.
- Measure changes with realistic traffic.

Slide content:

- Safety
- - acks=all plus min ISR protects acknowledged writes.; - enable.idempotence avoids retry duplicates in Kafka.; - Check send results; do not ignore async errors.
- Speed
- - batch.size controls batch size limits.; - linger.ms allows time to build batches.; - compression.type reduces network and disk use.
- Limits
- - delivery.timeout.ms bounds delivery waiting.; - Idempotent producer does not deduplicate business orders.; - Measure changes with realistic traffic.

## 26. Retention and compaction

Deletion is asynchronous and segment based, so retention is not an exact per-record timer. Compaction eventually retains the latest value per key and can leave older values until cleaning. A null value with a key is a tombstone; tombstones have their own retention. Compact and delete policies can be combined.

Slide content:

- Time/size retention
- - Keep records for a configured time or space.; - Old data expires whether read or unread.; - Choose retention for replay and storage needs.
- Log compaction
- - Keep the latest value for each key over time.; - Useful for the latest state of an entity.; - It is not the same as a normal database table.
- Plan storage
- - Estimate event size and incoming rate.; - Include replicas in disk estimates.; - Watch disk usage and consumer lag.

## 27. Event-driven architecture with Kafka

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Order API Publishes one order orders topic One stored record Notifications Own consumer group Inventory Own consumer group Both groups read the same event.

Inventory publishes a new event. Shipping listens to it.

Slide content:

- Order API
- Publishes one order
- orders topic
- One stored record
- Notifications
- Own consumer group
- Inventory
- Own consumer group
- Both groups read the same event.; ; Inventory publishes a new event. Shipping listens to it.

## 28. Event-driven: good habits

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Name events as facts - OrderCreated, PaymentReceived.
- Use past-tense names where practical.
- Do not expose internal database objects as events. Expect repeats - A record may be delivered again after a crash.
- Make important processing idempotent.
- Use an event ID or business key to detect duplicates. Plan for change - Version event schemas carefully.
- Keep consumers tolerant of compatible changes.
- Track event ownership and meaning.

Slide content:

- Name events as facts
- - OrderCreated, PaymentReceived.; - Use past-tense names where practical.; - Do not expose internal database objects as events.
- Expect repeats
- - A record may be delivered again after a crash.; - Make important processing idempotent.; - Use an event ID or business key to detect duplicates.
- Plan for change
- - Version event schemas carefully.; - Keep consumers tolerant of compatible changes.; - Track event ownership and meaning.

## 29. Event-driven workflows and eventual consistency

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. - OrderCreated -> StockReserved -> ShipmentPrepared.
- Each step publishes its result.
- Other services react when results arrive. Expect time gaps - HTTP 202 means accepted, not business completion.
- Show pending status while work continues.
- Use event IDs to trace a full journey. When a step fails - A saga can coordinate several business steps.
- A compensation may release reserved stock.
- A technical retry cannot fix every business failure.

Slide content:

- A chain of facts
- - OrderCreated -> StockReserved -> ShipmentPrepared.; - Each step publishes its result.; - Other services react when results arrive.
- Expect time gaps
- - HTTP 202 means accepted, not business completion.; - Show pending status while work continues.; - Use event IDs to trace a full journey.
- When a step fails
- - A saga can coordinate several business steps.; - A compensation may release reserved stock.; - A technical retry cannot fix every business failure.

## 30. Events and the database: avoid lost updates

Explain each idea using the same order example. Pause and ask the audience for one example from their own system.  an order, then publish an event.
- A crash between the two can lose the event.
- Publishing first can announce an unsaved order. Transactional outbox - Save the order and an outbox row together.
- A relay or CDC connector publishes outbox rows.
- Use event IDs to make repeated delivery safe. Demo boundary - This demo has no database or real side effects.
- One app contains several logical services.
- Production services can deploy independently.

Slide content:

- The dual-write problem
- - Save an order, then publish an event.; - A crash between the two can lose the event.; - Publishing first can announce an unsaved order.
- Transactional outbox
- - Save the order and an outbox row together.; - A relay or CDC connector publishes outbox rows.; - Use event IDs to make repeated delivery safe.
- Demo boundary
- - This demo has no database or real side effects.; - One app contains several logical services.; - Production services can deploy independently.

## 31. Delivery guarantees in plain words

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. cord is handled zero or one time.
- Can lose work if progress is saved too early.
- Useful only when occasional loss is acceptable. At-least-once - Retry until work succeeds.
- A record can be seen more than once.
- Common choice; make processing idempotent. Exactly-once - Kafka transactions can make Kafka-to-Kafka steps atomic.
- This does not magically make an external database/API exactly once.
- External side effects still need a safe design.

Slide content:

- At-most-once
- - A record is handled zero or one time.; - Can lose work if progress is saved too early.; - Useful only when occasional loss is acceptable.
- At-least-once
- - Retry until work succeeds.; - A record can be seen more than once.; - Common choice; make processing idempotent.
- Exactly-once
- - Kafka transactions can make Kafka-to-Kafka steps atomic.; - This does not magically make an external database/API exactly once.; - External side effects still need a safe design.

## 32. Transactions and exactly-once: know the scope

The sample is intentionally non-transactional. A crash after publishing inventory.reserved but before committing orders can publish the second event again. A crash between DLT publish and source commit can duplicate DLT records. Never call this sample exactly once.

Slide content:

- Kafka transaction
- - Can publish records and consumed offsets atomically.; - Consumers use read_committed to hide aborted records.; - Useful for Kafka-to-Kafka workflows.
- Outside Kafka
- - An email or payment API is a separate side effect.; - Kafka transactions do not make it exactly once.; - Use idempotency keys and a safe storage design.
- This lab
- - Uses ordinary at-least-once processing.; - A crash can repeat shipping or DLQ records.; - Logs simulate work; there is no payment or email API.

## 33. DLQ: what it is and what it is not

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. ue - In Kafka, it is commonly another topic.
- Example: orders.DLT.
- It holds records that could not be processed normally. Kafka does not do it automatically - Your consumer/framework detects the failure.
- It can retry, then publish to the DLQ.
- Spring Kafka provides a recoverer for this pattern. A DLQ is not a trash can - Keep the original event and failure details.
- Alert and inspect the DLQ.
- Fix, replay, or safely discard with a reason.

Slide content:

- DLQ means dead-letter queue
- - In Kafka, it is commonly another topic.; - Example: orders.DLT.; - It holds records that could not be processed normally.
- Kafka does not do it automatically
- - Your consumer/framework detects the failure.; - It can retry, then publish to the DLQ.; - Spring Kafka provides a recoverer for this pattern.
- A DLQ is not a trash can
- - Keep the original event and failure details.; - Alert and inspect the DLQ.; - Fix, replay, or safely discard with a reason.

## 34. Retry choices: temporary or permanent failure?

Amount 0 is a deliberate permanent failure. Retrying it is only a teaching device. Real systems usually classify it as non-retryable. The inventory-specific factory prevents unrelated listeners from sharing the same DLQ policy. Deserialization failures require additional ErrorHandlingDeserializer configuration and a raw-byte-compatible recoverer, beyond this demo.

Slide content:

- Temporary failure
- - A network or service may recover soon.; - Retry a limited number of times with backoff.; - The lab uses 1 attempt plus 2 retries.
- Permanent failure
- - Bad data usually fails again unchanged.; - Route it for inspection without endless retry.; - Amount 0 is retried only to make the lab visible.
- Two retry styles
- - Blocking retries delay work in that partition.; - Retry topics let other work move forward.; - Retry topics can change processing order.

## 35. A simple retry-to-DLQ flow

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Read event Consumer gets a record Try work Processing fails Retry Try again with a limit orders.DLT Inspect and recover In the demo: one first attempt plus two retries, then the record goes to orders.DLT.

Slide content:

- Read event
- Consumer gets a record
- Try work
- Processing fails
- Retry
- Try again with a limit
- orders.DLT
- Inspect and recover
- In the demo: one first attempt plus two retries, then the record goes to orders.DLT.

## 36. DLQ operations: inspect, repair, replay

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Keep enough context - Original payload and key.
- Source topic, partition, and offset.
- Exception details and a correlation ID. Recover safely - Alert someone and find the cause.
- Fix code or validate a corrected event.
- Replay carefully with duplicate protection. Avoid new problems - Do not replay blindly into the same failure.
- Set DLQ retention and access rules.
- If DLQ publishing fails, do not skip the source record.

Slide content:

- Keep enough context
- - Original payload and key.; - Source topic, partition, and offset.; - Exception details and a correlation ID.
- Recover safely
- - Alert someone and find the cause.; - Fix code or validate a corrected event.; - Replay carefully with duplicate protection.
- Avoid new problems
- - Do not replay blindly into the same failure.; - Set DLQ retention and access rules.; - If DLQ publishing fails, do not skip the source record.

## 37. The Spring Boot demo

These are logical services in one JVM so learners need only one application. Inventory publishes a second event after successful processing. Shipping listens only to inventory.reserved. The exact same OrderEvent payload is reused to keep the demo small; production would define explicit event types and IDs.

Slide content:

- Order API
- Publishes one order
- orders topic
- One stored record
- Notifications
- Own consumer group
- Inventory
- Own consumer group
- Both groups read the same event.; ; Inventory publishes a new event. Shipping listens to it.

## 38. Run the demo

Open docs/DEMO-GUIDE.md. Start healthy Kafka, start the app, send amount 25.50, then amount 0. For the failed order count three inventory-attempt entries and one DLQ entry, plus notification. There must be no shipping entry for that failed order. Startup may take time for group assignment.

Slide content:

- Start Kafka
- - Run: docker compose up -d --wait; - One local broker starts.; - Wait for the broker before starting the app.
- Start Spring Boot
- - In demo/: mvn spring-boot:run; - POST an order with amount 25.50.; - See notification and inventory logs.
- Trigger the DLQ
- - POST another order with amount 0.; - Inventory retries, then fails.; - See the orders.DLT log.

## 39. Run the three-broker and two-app labs

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Three brokers - Use compose.cluster.yaml.
- Bootstrap ports: 29092, 29093, 29094.
- Start app with replication factor 3. Two Spring Boot instances - Use ports 8080 and 8081.
- Keep the same listener group IDs.
- Three partitions are shared across the two apps. Verify - Describe partitions, leaders, and ISR.
- Inspect the consumer groups and lag.
- Stop a broker, publish again, then restart it.

Slide content:

- Three brokers
- - Use compose.cluster.yaml.; - Bootstrap ports: 29092, 29093, 29094.; - Start app with replication factor 3.
- Two Spring Boot instances
- - Use ports 8080 and 8081.; - Keep the same listener group IDs.; - Three partitions are shared across the two apps.
- Verify
- - Describe partitions, leaders, and ISR.; - Inspect the consumer groups and lag.; - Stop a broker, publish again, then restart it.

## 40. Advanced Kafka features

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. ent streams.
- Filter, transform, join, and aggregate.
- Can keep local state and recover it. Producer features - Idempotence helps prevent duplicate writes from producer retries.
- Transactions can make Kafka-to-Kafka writes atomic.
- Batching and compression can improve efficiency. Platform tools - Schema Registry is a commonly used separate service.
- MirrorMaker 2 can copy data between clusters.
- Modern Kafka uses KRaft for cluster metadata.

Slide content:

- Kafka Connect
- - Moves data between Kafka and other systems.; - Source connectors bring data in.; - Sink connectors send data out.
- Kafka Streams
- - Java library for processing event streams.; - Filter, transform, join, and aggregate.; - Can keep local state and recover it.
- Producer features
- - Idempotence helps prevent duplicate writes from producer retries.; - Transactions can make Kafka-to-Kafka writes atomic.; - Batching and compression can improve efficiency.
- Platform tools
- - Schema Registry is a commonly used separate service.; - MirrorMaker 2 can copy data between clusters.; - Modern Kafka uses KRaft for cluster metadata.

## 41. Kafka Connect and change data capture

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. ect - A separate runtime for source and sink connectors.
- A source writes to Kafka; a sink reads from Kafka.
- Distributed workers share connector tasks. CDC - Change data capture watches database changes.
- A connector such as Debezium can publish them.
- Often used to relay an outbox reliably. Plan the details - Connectors must be installed and configured.
- Compatibility and delivery guarantees vary.
- Monitor task failures and connector lag.

Slide content:

- Connect
- - A separate runtime for source and sink connectors.; - A source writes to Kafka; a sink reads from Kafka.; - Distributed workers share connector tasks.
- CDC
- - Change data capture watches database changes.; - A connector such as Debezium can publish them.; - Often used to relay an outbox reliably.
- Plan the details
- - Connectors must be installed and configured.; - Compatibility and delivery guarantees vary.; - Monitor task failures and connector lag.

## 42. Kafka Streams, windows, and state

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Simple processing - Filter invalid events.
- Transform an order into another event.
- Group and count orders per customer. Time-based work - A window groups events over a time range.
- Event time can differ from arrival time.
- Grace periods allow some late events. State and joins - State stores keep running totals or lookup data.
- Changelog topics help rebuild state.
- Joins may require matching partitioning.

Slide content:

- Simple processing
- - Filter invalid events.; - Transform an order into another event.; - Group and count orders per customer.
- Time-based work
- - A window groups events over a time range.; - Event time can differ from arrival time.; - Grace periods allow some late events.
- State and joins
- - State stores keep running totals or lookup data.; - Changelog topics help rebuild state.; - Joins may require matching partitioning.

## 43. Schemas and event evolution

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. A contract - A schema describes the event fields.
- JSON, Avro, and Protobuf are common choices.
- Document field meaning, not only field types. Change safely - Prefer compatible additions with defaults.
- Do not casually rename or remove fields.
- Test old readers with new records and vice versa. Schema Registry - A commonly added separate service.
- Stores schemas and checks compatibility rules.
- It is not built into the Apache Kafka broker.

Slide content:

- A contract
- - A schema describes the event fields.; - JSON, Avro, and Protobuf are common choices.; - Document field meaning, not only field types.
- Change safely
- - Prefer compatible additions with defaults.; - Do not casually rename or remove fields.; - Test old readers with new records and vice versa.
- Schema Registry
- - A commonly added separate service.; - Stores schemas and checks compatibility rules.; - It is not built into the Apache Kafka broker.

## 44. Security and data protection

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Protect the connection - TLS encrypts traffic.
- SASL or certificates identify clients.
- ACLs control who can read or write topics. Protect the data - Keep secrets out of event payloads.
- Plan retention, deletion, and access reviews.
- Limit who can read failure details in a DLQ. Lab vs production - Local labs use plaintext for simplicity.
- Use private networking and secure listeners at work.
- Do not expose these lab ports publicly.

Slide content:

- Protect the connection
- - TLS encrypts traffic.; - SASL or certificates identify clients.; - ACLs control who can read or write topics.
- Protect the data
- - Keep secrets out of event payloads.; - Plan retention, deletion, and access reviews.; - Limit who can read failure details in a DLQ.
- Lab vs production
- - Local labs use plaintext for simplicity.; - Use private networking and secure listeners at work.; - Do not expose these lab ports publicly.

## 45. Monitoring and performance

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Watch consumers - Lag and event age.
- Processing time, retries, and DLQ rate.
- Frequent rebalances or stalled polling. Watch brokers - Disk space and storage growth.
- Under-replicated or offline partitions.
- Request latency and controller quorum health. Improve carefully - Check skew: one hot key can overload a partition.
- Estimate storage: bytes/sec x retention x replicas.
- Leave space for overhead and recovery.

Slide content:

- Watch consumers
- - Lag and event age.; - Processing time, retries, and DLQ rate.; - Frequent rebalances or stalled polling.
- Watch brokers
- - Disk space and storage growth.; - Under-replicated or offline partitions.; - Request latency and controller quorum health.
- Improve carefully
- - Check skew: one hot key can overload a partition.; - Estimate storage: bytes/sec x retention x replicas.; - Leave space for overhead and recovery.

## 46. Replay and disaster recovery

auto.offset.reset=earliest is a fallback for missing or out-of-range committed positions. It does not rewind an established group on every restart. Kafka replication can replicate deletions too; it is not a substitute for retention planning or backups.

Slide content:

- Replay retained records
- - Use a new group to read retained history.; - Or stop a group and reset its offsets carefully.; - earliest is used only when no valid offset exists.
- Limits
- - Expired records cannot be replayed from Kafka.; - Compacted logs may not contain every old value.; - Replay can repeat real business actions.
- Another cluster
- - MirrorMaker 2 can replicate between clusters.; - Replication is usually asynchronous.; - Plan failover, offsets, and possible gaps or repeats.

## 47. Quick knowledge check

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. Ask the audience - Two different groups: who sees the order?
- Four consumers and three partitions: who works?
- Does reading delete a Kafka record? More questions - If committed offset is 8, what reads next?
- Does Kafka route failures to a DLQ by itself?
- Does one broker give high availability? Answers - Both groups; at most three active consumers.
- No deletion; next offset is 8 if retained.
- No automatic DLQ; no single-broker failover.

Slide content:

- Ask the audience
- - Two different groups: who sees the order?; - Four consumers and three partitions: who works?; - Does reading delete a Kafka record?
- More questions
- - If committed offset is 8, what reads next?; - Does Kafka route failures to a DLQ by itself?; - Does one broker give high availability?
- Answers
- - Both groups; at most three active consumers.; - No deletion; next offset is 8 if retained.; - No automatic DLQ; no single-broker failover.

## 48. Production checklist and recap

Explain each idea using the same order example. Pause and ask the audience for one example from their own system. esign - Pick meaningful event names and keys.
- Set partitions, retention, and schema rules.
- Make consumers idempotent and safe to replay. Operate - Use multiple brokers and suitable replication.
- Secure with TLS, authentication, and ACLs.
- Monitor lag, disk, under-replicated partitions, and DLQ. Remember - Kafka stores and distributes event streams.
- Groups decide fan-out versus shared work.
- DLQ handling is configured by your application.

Slide content:

- Design
- - Pick meaningful event names and keys.; - Set partitions, retention, and schema rules.; - Make consumers idempotent and safe to replay.
- Operate
- - Use multiple brokers and suitable replication.; - Secure with TLS, authentication, and ACLs.; - Monitor lag, disk, under-replicated partitions, and DLQ.
- Remember
- - Kafka stores and distributes event streams.; - Groups decide fan-out versus shared work.; - DLQ handling is configured by your application.

## Official references

- https://kafka.apache.org/40/getting-started/introduction/
- https://kafka.apache.org/40/design/design/
- https://kafka.apache.org/40/getting-started/upgrade/
- https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
- https://kafka.apache.org/40/streams/
- https://kafka.apache.org/40/kafka-connect/
