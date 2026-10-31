#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
# Admin identity can set ACLs. Alice gets only describe/write on this topic and idempotent write.
# Bob gets describe only so metadata is visible but an actual write is rejected by Kafka.
for user in alice bob; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka:19092 --command-config /etc/kafka/admin.properties --add --allow-principal "User:$user" --operation Describe --topic sample.secure.orders
 done
docker compose exec -T kafka /opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka:19092 --command-config /etc/kafka/admin.properties --add --allow-principal User:alice --operation Write --topic sample.secure.orders
docker compose exec -T kafka /opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka:19092 --command-config /etc/kafka/admin.properties --add --allow-principal User:alice --operation IdempotentWrite --cluster
docker compose exec -T kafka /opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka:19092 --command-config /etc/kafka/admin.properties --add --allow-principal User:bob --operation IdempotentWrite --cluster
