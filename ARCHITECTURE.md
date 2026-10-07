# FraudX Architecture

This is the **authoritative architecture** of FraudX. `FraudX_README.md` is the
original specification; where the two differ, this document wins and the
reason is recorded as an ADR in [`memory.md`](memory.md#architectural-decisions).

## 1. Objective

Detect suspicious financial transactions in near real time by combining
polyglot NoSQL storage, stream processing, supervised ML, anomaly detection,
graph analysis and evidence-grounded explanations. Every technology has a
single, defensible responsibility.

## 2. Data flow

```text
 PaySim CSV / built-in synthetic generator
                 │  producer (keyed by customer_id)
                 ▼
        ┌──────────────────┐   invalid    ┌─────────────────┐
        │ transactions.raw ├─────────────►│ transactions.dlq│
        └────────┬─────────┘              └─────────────────┘
                 │  feature-processor (consumer group, N replicas)
                 │   validate → normalise → sliding-window features
                 │   read/write online features ◄──────────────► REDIS
                 ▼
        ┌───────────────────────┐
        │ transactions.features │──────────► graph-writer ──► NEO4J
        └────────┬──────────────┘            (entities + aggregated edges)
                 │  scorer (consumer group)
                 │   rules + ML model + anomaly model
                 │   graph features  ◄──────────────────────── NEO4J
                 │   risk engine (config/risk.yaml)
                 ▼
        ┌──────────────────────┐
        │ transactions.scored  │──┬──► history-writer ──► CASSANDRA
        └──────────────────────┘  ├──► profile-writer ──► MONGODB (profiles)
                 │                └──► latest risk  ───► REDIS
                 │ score ≥ alert_threshold
                 ▼
        ┌──────────────┐
        │ fraud.alerts │──► alert-dispatcher (webhook / Slack / email)
        └──────┬───────┘──► explainer: evidence retrieval ─► template / LLM
               │                                          └► MONGODB (investigations)
               ▼
   FastAPI  (reads all four stores; SSE live stream) ──► React dashboard
   Prometheus ◄── /metrics of every service ──► Grafana
```

Each arrow out of a topic is an **independent consumer group**: a slow or
failed sink never blocks scoring, and any stage can be replayed from Kafka.

## 3. Components

| Component | Status | Responsibility |
|---|---|---|
| `ingestion` datasets | **done** | Deterministic synthetic generator with labelled fraud scenarios; PaySim loader with label-independent enrichment (§12) |
| `ingestion` producer | Phase 2 | Replay a dataset to `transactions.raw` at a controlled rate |
| `streaming` feature-processor | Phase 3 | Validate against `TransactionEvent`, dead-letter invalid events, compute online features in Redis, publish `transactions.features` |
| `storage` writers | Phase 4 | Idempotent sinks: Cassandra history, MongoDB profiles, Neo4j graph |
| `scoring` scorer | Phase 5–8 | Rules + XGBoost/LightGBM + Isolation Forest + graph features → risk engine |
| `scoring.risk` | **done** | Weighted, configurable 0–100 score with rule floors and missing-signal handling |
| `alerts` | Phase 8 | Deliver alerts from `fraud.alerts` |
| `explain` | Phase 9 | Evidence retrieval → deterministic template or LLM explanation |
| `api` | **health done** | REST API aggregating the stores; `/health`, `/health/ready`, `/metrics` |
| frontend | Phase 10 | React + TypeScript dashboard |

All Python services are entrypoints of **one package** (`src/fraudx`) built into
**one image**; compose selects the command. Shared contracts
(`fraudx.core.schemas`), settings and client factories exist once.

## 4. Kafka

Single KRaft broker locally (no ZooKeeper). Topic auto-creation is disabled;
`infra/kafka/create-topics.sh` provisions topics, and the integration suite
checks the topics match `fraudx.core.topics`.

| Topic | Key | Partitions | Retention | Producer → consumers |
|---|---|---|---|---|
| `transactions.raw` | customer_id | 6 | 7 d | producer → feature-processor |
| `transactions.features` | customer_id | 6 | 7 d | feature-processor → scorer, graph-writer |
| `transactions.scored` | customer_id | 6 | 30 d | scorer → history/profile writers, API live stream |
| `transactions.dlq` | — | 1 | 30 d | any stage → operators (error + original payload) |
| `fraud.alerts` | customer_id | 3 | 30 d | scorer → alert-dispatcher, explainer |

* **Keying by `customer_id`** preserves per-customer order, which the velocity
  windows need; the keyed pipeline topics share a partition count so a customer
  stays on the same partition at every stage.
* **Delivery:** at-least-once (commit after processing) combined with
  idempotent writes everywhere (natural keys, `ZADD` by transaction id,
  Cassandra upserts, Neo4j `MERGE`). Replays are therefore safe.
* **Serialization:** JSON validated by versioned Pydantic models
  (`schema_version`). Changes are additive only; see ADR-010.
* Production: RF=3, `min.insync.replicas=2`, producer `acks=all` + idempotence.

## 5. Data stores: why each one exists

| Store | Data | Why this model |
|---|---|---|
| **Redis** | Online features, latest risk per customer, dedupe keys | Sub-millisecond reads/writes on the hot path; sorted sets give exact sliding windows |
| **Cassandra** | Full transaction history, risk event feed | Write-heavy, append-only time series at volume; partitioned by query |
| **MongoDB** | Customer/account/device profiles, investigations, model registry | Flexible, nested, slowly changing documents read whole by the API |
| **Neo4j** | Customers, accounts, devices, IPs, locations and their links | Multi-hop relationship queries (shared devices, fraud rings) |

### 5.1 Cassandra (`infra/cassandra/schema.cql`)

One table per query; money is `decimal`; newest-first clustering.

| Table | Partition key | Clustering | Serves |
|---|---|---|---|
| `transactions_by_customer` | `(customer_id, event_date)` | `event_time DESC, transaction_id` | customer history for a day / range |
| `transactions_by_id` | `transaction_id` | — | `GET /transactions/{id}` |
| `risk_events_by_day` | `(event_date, bucket)` | `event_time DESC, transaction_id` | alert feed; `bucket = hash(tx) % 8` avoids a hot daily partition |

Time-series tables use `TimeWindowCompactionStrategy` (1-day windows).
Writers use `LOCAL_QUORUM` in production (RF=3).

### 5.2 MongoDB (`infra/mongodb/init.js`)

Natural keys are stored as `_id`. Collections have `$jsonSchema` validators
(`validationLevel: moderate`). The API connects as a least-privilege user
(`readWrite` on `fraudx` only).

| Collection | `_id` | Notable fields / indexes |
|---|---|---|
| `customers` | customer_id | `devices`, `known_ips` (capped at 50), `risk_profile{score,level}`; index `(risk_profile.level, risk_profile.score)` |
| `accounts` | account_id | `customer_id` (indexed), `kind` CUSTOMER/MERCHANT |
| `devices` | device_id | first/last seen, metadata |
| `investigations` | transaction_id | risk breakdown, `evidence`, `explanation`, `status`; indexes `(status, created_at)`, `(customer_id, created_at)` |
| `model_registry` | `name:version` | metrics, params, artifact path; partial unique index → one active version per model |

### 5.3 Redis key design

All keys of one customer share a hash tag `{customer_id}` so multi-key
operations stay valid on Redis Cluster. Every key has a TTL.

| Key | Type | Content |
|---|---|---|
| `fx:cust:{C}:txw` | ZSET | member `tx_id\|amount`, score = event time ms (sliding windows 5 m / 1 h / 24 h) |
| `fx:cust:{C}:stats` | HASH | count, amount sum, sum of squares (mean / std), last device / IP / country / time |
| `fx:cust:{C}:devices` / `:ips` / `:countries` | SET | known values → `new_device`, `new_ip`, `new_location` |
| `fx:cust:{C}:risk` | HASH | latest score, level, transaction id |
| `fx:dev:{D}:txw`, `fx:ip:{IP}:txw` | ZSET | device / IP velocity |
| `fx:seen:{tx_id}` | STRING (NX, 24 h) | dedupe guard so replays don't double-count `HINCRBY` |

Windows use **event time** (the transaction timestamp), so replays and
backfills produce the same features as live traffic.

### 5.4 Neo4j (`infra/neo4j/schema.cypher`)

```text
(:Customer)-[:OWNS]->(:Account)
(:Customer)-[:USES {first_seen,last_seen,count}]->(:Device)
(:Device)-[:CONNECTED_FROM {first_seen,last_seen,count}]->(:IP)
(:IP)-[:LOCATED_IN]->(:Location)
(:Account)-[:TRANSFERRED_TO {count,total_amount,first_seen,last_seen}]->(:Account)
```

Transactions are **aggregated onto edges**, not stored as nodes (ADR-004);
the graph grows with entities, not events. Merchants are `Account` nodes with
`kind = "MERCHANT"`. `fraud_flag` on Customer/Account (from labels or analyst
confirmation) drives `fraud_neighbor_count` and `distance_to_known_fraud`.
Uniqueness constraints on every id back the `MERGE` writes.

## 6. Scoring

```text
score = round_half_up(100 × Σ wᵢ·sᵢ / Σ wᵢ)   over the available signals i ∈ {ml, anomaly, graph}
score = max(score, floor of every rule hit)
level = HIGH if score ≥ 70, MEDIUM if ≥ 30, else LOW         (config/risk.yaml)
```

* Weights and thresholds come from `config/risk.yaml`, validated at load
  (weights sum to 1, thresholds ordered).
* A missing signal (model not loaded, graph timeout) has its weight
  redistributed and is listed in `missing_signals`. It is never counted as 0.
* Deterministic rules (blocklists, impossible travel, …) set a minimum score.
* ML training is offline (`ml` scripts → `models/` + `model_registry`); the
  scorer only loads a registered, versioned artifact.
* Evaluation uses precision, recall, F1, ROC-AUC and **PR-AUC (primary)** with a
  time-based split. Accuracy is not used because of class imbalance.

## 7. Explanations (LLM)

* Explanations are generated only for alerts (score ≥ threshold) or on analyst
  request, never for every event.
* The pipeline is retrieval → evidence object (facts with their source store)
  → prompt → structured output (`summary`, `reasons[]`, each reason citing
  evidence keys). Any reason that doesn't cite existing evidence is dropped.
* Default provider `template` is deterministic and works offline. LLM providers
  are opt-in behind the same interface.
* Data minimisation: identifiers are pseudonymised and raw IPs / balances are
  not sent to an external LLM.

## 8. Security

* No secrets in code or images; `.env` (git-ignored) supplies credentials and
  compose fails fast when a required one is missing.
* All host ports bind to `127.0.0.1`.
* Authentication is on for MongoDB (root + least-privilege app user), Redis
  (`requirepass`) and Neo4j. Cassandra auth is off locally (known issue, see
  memory.md).
* Containers run as a non-root user (uid 10001).
* `is_fraud_label` is excluded from online scoring to prevent label leakage.

## 9. Observability

* Every service exposes Prometheus metrics; the API has
  `fraudx_http_requests_total` and `fraudx_http_request_duration_seconds`,
  labelled by route template.
* Pipeline metrics (to be added per phase): events/s per stage, processing
  latency, end-to-end latency (`now − event_time`), DLQ count, model inference
  latency, Kafka consumer lag (kafka-exporter).
* JSON logs on stdout with contextual fields.
* `/health` is liveness (no dependencies); `/health/ready` checks all five
  backends and the presence of the Kafka topics.

## 10. Scalability path

| Concern | Local | Scale-out |
|---|---|---|
| Kafka | 1 broker, 6 partitions | 3+ brokers, RF=3; up to 6 consumers per group without re-partitioning |
| Stream processing | 1 feature-processor | N replicas in one consumer group; state lives in Redis, so workers are stateless |
| Redis | single node | Redis Cluster (keys are already hash-tagged per customer) |
| Cassandra | 1 node, RF=1 | multi-node, NetworkTopologyStrategy RF=3 |
| MongoDB | standalone | replica set, then shard `investigations` by `customer_id` if needed |
| Neo4j | community single | Enterprise cluster / read replicas for graph-feature reads |
| Stateful streaming beyond Redis | — | Apache Flink (ADR-001 revisit criteria) |

## 11. Local deployment

`docker-compose.yml` runs Kafka, Kafka UI (kafbat), MongoDB, Cassandra,
Redis, Neo4j, one-shot provisioning jobs (`kafka-init`, `cassandra-init`,
`neo4j-init`), the API, Prometheus and Grafana. Every long-running service
has a healthcheck; `docker compose up -d --wait` returns once all are healthy
(about 1–2 minutes after the first image pull). Kubernetes is out of scope for
the MVP.

## 12. Datasets

`python -m fraudx.ingestion generate|paysim` (or `make data` / `make data-paysim`)
writes a dataset directory:

| File | Content |
|---|---|
| `enriched_transactions.jsonl` | `TransactionEvent` v1 per line, event-time order, validated on write |
| `ground_truth.csv` | `transaction_id, is_fraud, scenario`, for evaluation only, never published to Kafka |
| `summary.json` | counts by type/scenario, fraud rate, time range, generation parameters / invalid rows |

**Synthetic generator** (`fraudx.ingestion.generator`). It is a pure function
of its config and seed. Population: customers with home country, devices, IPs,
spending profile and contacts; merchants; fraud rings whose mules come from the
population and share ring devices and IPs. Generation runs in two passes:
transactions are planned with timestamps, then sorted and replayed against
account balances, so balances are always consistent.

| Scenario | Label | Behaviour |
|---|---|---|
| `account_takeover` | fraud | TRANSFER of 80–100 % of the victim's balance to a mule, from a ring device/IP, mostly at night |
| `mule_cash_out` | fraud | the mule CASH_OUTs 90–100 % within 5–60 min, same ring device |
| `card_testing` | fraud | 5–15 payments of 1–20 TND seconds apart from an unknown/ring device abroad, then one large payment |
| `travel` | legit | days abroad with foreign IPs/locations |
| `new_device` | legit | a new phone that becomes a known device |
| `large_purchase` | legit | an occasional purchase 10–30× the usual amount |
| (shared devices) | legit | ~2 % of customers share a device with family |

Defaults (2,000 customers, 14 days) give about 64k events at about 0.5 % fraud in
about 2.5 s.

**PaySim loader** (`fraudx.ingestion.paysim`). It maps `step` → event time,
`nameOrig` → customer/source account and `isFraud` → label. Device, IP and
location come from a salted hash of the customer id, so they're
label-independent and add no signal to PaySim.
