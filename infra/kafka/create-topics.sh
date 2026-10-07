#!/usr/bin/env bash
# Idempotently creates FraudX topics. Runs once as the `kafka-init` container.
# Keep in sync with src/fraudx/core/topics.py (checked by integration tests).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:19092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-6}"
RF="${KAFKA_REPLICATION_FACTOR:-1}"
DAY_MS=86400000
TOPICS=/opt/kafka/bin/kafka-topics.sh

create() {  # name partitions retention_ms
  "$TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor "$RF" \
    --config retention.ms="$3" --config cleanup.policy=delete
}

# Keyed by customer_id: same partition count on every keyed topic keeps a
# customer's events co-partitioned across pipeline stages.
create transactions.raw      "$PARTITIONS" $((7 * DAY_MS))
create transactions.features "$PARTITIONS" $((7 * DAY_MS))
create transactions.scored   "$PARTITIONS" $((30 * DAY_MS))
create transactions.dlq      1             $((30 * DAY_MS))
create fraud.alerts          3             $((30 * DAY_MS))

"$TOPICS" --bootstrap-server "$BOOTSTRAP" --list
