-- FraudX Cassandra schema: transaction history (append-only, high volume).
-- One table per query. Applied idempotently by the `cassandra-init` container.
-- Local dev uses SimpleStrategy/RF=1; production: NetworkTopologyStrategy, RF=3,
-- reads/writes at LOCAL_QUORUM.

CREATE KEYSPACE IF NOT EXISTS fraudx
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Q1: a customer's transactions for a day / time range, newest first.
--     Partition = one customer-day, bounded in size; multi-day ranges query
--     one partition per day.
CREATE TABLE IF NOT EXISTS fraudx.transactions_by_customer (
    customer_id         text,
    event_date          date,
    event_time          timestamp,
    transaction_id      text,
    source_account      text,
    destination_account text,
    transaction_type    text,
    amount              decimal,
    currency            text,
    device_id           text,
    ip_address          inet,
    country             text,
    city                text,
    risk_score          smallint,
    risk_level          text,
    ml_score            float,
    anomaly_score       float,
    graph_score         float,
    PRIMARY KEY ((customer_id, event_date), event_time, transaction_id)
) WITH CLUSTERING ORDER BY (event_time DESC, transaction_id ASC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};

-- Q2: point lookup of one transaction (GET /transactions/{id}).
CREATE TABLE IF NOT EXISTS fraudx.transactions_by_id (
    transaction_id      text PRIMARY KEY,
    customer_id         text,
    event_date          date,
    event_time          timestamp,
    source_account      text,
    destination_account text,
    transaction_type    text,
    amount              decimal,
    currency            text,
    device_id           text,
    ip_address          inet,
    country             text,
    city                text,
    risk_score          smallint,
    risk_level          text,
    ml_score            float,
    anomaly_score       float,
    graph_score         float,
    rule_hits           frozen<list<text>>,  -- frozen: written as one value, no tombstone
    is_fraud_label      boolean
);

-- Q3: risky transactions of a day, newest first (alert feed, GET /fraud/alerts).
--     `bucket` (hash(transaction_id) % 8) spreads a busy day over 8
--     partitions; readers query all buckets and merge.
CREATE TABLE IF NOT EXISTS fraudx.risk_events_by_day (
    event_date       date,
    bucket           tinyint,
    event_time       timestamp,
    transaction_id   text,
    customer_id      text,
    amount           decimal,
    transaction_type text,
    risk_score       smallint,
    risk_level       text,
    rule_hits        frozen<list<text>>,
    PRIMARY KEY ((event_date, bucket), event_time, transaction_id)
) WITH CLUSTERING ORDER BY (event_time DESC, transaction_id ASC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};
