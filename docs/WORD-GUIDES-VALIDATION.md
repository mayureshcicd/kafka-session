# Word guides and Java-comment verification

Updated on 31 October 2026.

- One REAL-LIFE-GUIDE.docx exists in each of the 14 numbered projects and the original combined demo: 15 files.
- Each guide has a distinct practical situation, the reason to use the feature, a step-by-step flow, failure behavior, the actual Java-file walkthrough, run commands, and a glossary.
- Each guide contains approximately 1,200–1,400 words in easy language.
- All 15 Word files opened and rendered with LibreOffice. Extracted text was checked against the body paragraphs to detect missing content.
- Reviewed a rendered code-walkthrough page visually; tables and readable fonts were checked.
- All 92 main Java source files have purpose comments. Additional method comments and expanded short methods explain intent without changing business logic.
- Maven packaging passed for projects 01–14 and for the original demo after the comment/layout changes.
- The original demo's four Java tests passed, with no failures or errors.
- These changes were comments, formatting, and documentation. The earlier Kafka live-check results remain recorded separately; the full live infrastructure suite was not rerun solely for these comment changes.

Rendered page counts:

| Project | Pages |
|---|---|
| 01-pub-sub-application | 4 |
| 02-event-driven-application | 4 |
| 03-dlq-application | 4 |
| 04-ordering-and-groups | 4 |
| 05-idempotent-loyalty | 5 |
| 06-non-blocking-retry | 5 |
| 07-streams-dashboard | 4 |
| 08-kafka-transactions | 5 |
| 09-outbox-cdc | 5 |
| 10-schema-evolution | 5 |
| 11-compaction-and-tombstones | 5 |
| 12-broker-failover | 4 |
| 13-lag-and-rebalancing | 4 |
| 14-secure-publishing | 5 |
| demo | 5 |
