# Session plan

## Audience and goal

Assume the audience knows basic HTTP and Java but has never used Kafka. Use one example throughout: an order is received, inventory reserves stock, and shipping reacts. Say each term in plain language before showing its technical name.

## Full workshop — about 100 minutes

| Time | Slides | Teaching goal |
|---|---|---|
| 0–12 min | 1–8 | What is an event? Why Kafka? Benefits, costs, and vocabulary |
| 12–25 min | 9–18 | Pub/sub, internals, partitions, offsets, lag, groups, rebalances |
| 25–40 min | 19–26 | One broker, a cluster, KRaft, replicas, leaders, durability, retention |
| 40–53 min | 27–32 | Event-driven workflows, outbox, delivery guarantees, transactions |
| 53–63 min | 33–36 | Retry decisions, DLQ, and recovery |
| 63–83 min | 37–39 | Live demo; optionally run two apps and fail one broker |
| 83–95 min | 40–46 | Connect, CDC, Streams, schemas, security, monitoring, recovery |
| 95–100 min | 47–48 | Audience questions and recap |

## Short session — 60 minutes

Use slides 1–12, 14–17, 19–24, 27, 30–31, 33–38, 40, and 47–48. Explain omitted slides are reference material. Aim for 40 minutes of explanation, 15 minutes of demonstration, and 5 minutes of questions. Do the three-broker failover exercise only if time permits.

## Preparation before presenting

1. Run the single-broker demo before the session. Send valid and invalid orders, then compare their journal entries with the expected behavior in DEMO-GUIDE.md.
2. Download dependencies and Docker images ahead of time.
3. Keep the deck, `DEMO-GUIDE.md`, a terminal showing app logs, and a curl terminal open.
4. Clear only your own lab volumes if you need a clean history. Persistent groups normally resume their bookmarks.
5. If using two applications, remember `/demo/status` shows only that instance's work. Inspect both ports.
6. Use `docs/VALIDATION.md` to see the environment checks already performed.

## Speak simply

- Topic: a named journal of related events.
- Partition: one ordered lane in that journal.
- Offset: a bookmark inside one lane.
- Consumer group: a team sharing lanes.
- Replica: another copy of a lane on another broker.
- Lag: how far a team's bookmark is behind.
- DLQ: a place to investigate records that failed.

Do not call Kafka an instant-response API. Do not promise every event is processed once. Do not say reading deletes a record. After explaining pub/sub, ask: “If email stops, can inventory continue?” Yes: they use different groups.
