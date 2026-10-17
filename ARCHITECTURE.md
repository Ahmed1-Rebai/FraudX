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
        │ fraud.alerts │──► alert-handler: evidence ─► template / Claude explanation
        └──────┬───────┘        └► MONGODB (investigations)  └► webhook (optional)
               ▼
   FastAPI  (reads all four stores; SSE live stream) ──► React dashboard
   Prometheus ◄── /metrics of every service ──► Grafana
```

Each arrow out of a topic is an **independent consumer group**: a slow or
failed sink never blocks scoring, and any stage can be replayed from Kafka.

The history and profile writers consume `transactions.scored` (ADR-021), so
every Cassandra row and MongoDB profile carries its risk. The graph writer
consumes `transactions.features`: the scorer reads the graph it builds.

## 3. Components

| Component | Status | Responsibility |
|---|---|---|
| `ingestion` datasets | **done** | Deterministic synthetic generator with labelled fraud scenarios (§12) |
| `datasets` | **done** | Public-dataset pipelines (PaySim, IBM AML): profile → clean → canonicalize → report (§12) |
| `ingestion` replay | **done** | Replay a dataset to `transactions.raw`: paced, keyed by customer, labels stripped, optional time rebase |
| `messaging` runtime | **done** | `EventProducer` + `StreamWorker`: the shared Kafka runtime for every stage (§4.1) |
| `streaming` feature-processor | **done** | Validate against `TransactionEvent`, dead-letter invalid events, compute online features in Redis (§5.3), publish `FeaturedTransaction` to `transactions.features` |
| `storage` writers | **done** | `graph-writer` (Neo4j), `history-writer` (Cassandra), `profile-writer` (MongoDB): batched, idempotent sinks, `python -m fraudx.storage graph\|history\|profile` |
| `ml` | **done** | Offline training (LightGBM) on features built by the online code; versioned artifacts + MongoDB registry; `FraudModel` for inference (§6.1) |
| `ml.anomaly` | **done** | Isolation Forest on legitimate traffic, calibrated to a percentile (§6.2) |
| `storage.graph_features` | **done** | Point-in-time graph features from Neo4j + graph model (§6.3) |
| `scoring` scorer | **done** | `python -m fraudx.scoring`: calibrated ml/anomaly/graph signals + rules → risk engine → `transactions.scored`, `fraud.alerts`, latest risk in Redis (§6.4) |
| `bench` | **done** | Storage benchmarks: MongoDB vs Cassandra (write/read latency, throughput, size), MongoDB vs Redis (`make bench`) |
| `scoring.risk` | **done** | Weighted, configurable 0–100 score with rule floors and missing-signal handling |
| `alerts` | **done** | `python -m fraudx.alerts`: `fraud.alerts` → MongoDB investigation with evidence + explanation, optional webhook (§7) |
| `explain` | **done** | Evidence → grounded explanation: deterministic template (default) or Claude (§7) |
| `api` | **done (Phase 8)** | REST API aggregating the stores (`storage.readers`): transactions and alert feed (Cassandra), profile (MongoDB), network (Neo4j), latest risk (Redis), `POST /transactions/simulate` (Kafka); `/health`, `/health/ready`, `/metrics` |
| `frontend` | **done** | React + TypeScript (Vite) dashboard served by nginx (`dashboard`, :8090), `/api` proxied to the API: overview KPIs, live stream (SSE), investigations with cited explanations and decisions, customer graph (§13) |

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
| `fraud.alerts` | customer_id | 3 | 30 d | scorer → alert-handler (investigation, explanation, webhook) |

* **Keying by `customer_id`** preserves per-customer order, which the velocity
  windows need; the keyed pipeline topics share a partition count so a customer
  stays on the same partition at every stage.
* **Delivery:** at-least-once (commit after processing) combined with
  idempotent writes everywhere (natural keys, `ZADD` by transaction id,
  Cassandra upserts, Neo4j `MERGE`). Replays are therefore safe.
* **Serialization:** JSON validated by versioned Pydantic models
  (`schema_version`). Changes are additive only; see ADR-010. Additive
  fields so far: `payment_method`, `original_amount`, `original_currency`
  (for multi-currency sources such as IBM AML).
* Production: RF=3, `min.insync.replicas=2`, producer `acks=all` + idempotence.

### 4.1 Worker runtime (`fraudx.messaging`)

Every pipeline stage is a `StreamWorker(handler)`. The handler holds the
business logic; the worker provides:

| Concern | Behaviour |
|---|---|
| Producer | idempotent, `acks=all`, lz4, `linger.ms=10`; waits (never drops) on a full local queue |
| Offsets | auto-commit off; an offset is recorded only after the handler succeeds; committed every 2 s / 500 msgs, on revoke and on shutdown, **after** `producer.flush()` |
| Poison messages | handler raises `PermanentError` (e.g. `decode()` fails validation) → original bytes + `dlq.*` headers to `transactions.dlq`, then continue |
| Transient failures | any other exception → seek back, exponential backoff up to 30 s, retry while still polling (stays in the group, no DLQ flood during an outage) |
| Batch mode | `batch_handler` gets up to `batch_size` (500) messages and returns its rejects for the DLQ; an exception rewinds every partition in the batch. Used by the store writers, whose writes are idempotent |
| Shutdown | SIGTERM/SIGINT → finish current message, commit, close |
| Metrics | `fraudx_consumer_messages_total{outcome}`, processing time, message age (produce→processed latency), `fraudx_consumer_lag` from librdkafka stats, `fraudx_kafka_produced_total` |
| Health | heartbeat file touched every poll loop; `python -m fraudx.messaging.heartbeat <file> 30` is the container healthcheck |

Ground-truth labels never travel on the live stream: `replay` strips
`is_fraud_label` unless `--with-labels` is passed for an evaluation run.

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
The history writer prepares keyspace-qualified statements, writes both
tables with concurrent async upserts at `LOCAL_QUORUM`, and binds missing
values as `UNSET` (never `null`). That avoids tombstones and means a later
stage's risk columns are never erased by a re-write.

### 5.2 MongoDB (`infra/mongodb/init.js`)

Natural keys are stored as `_id`. Collections have `$jsonSchema` validators
(`validationLevel: moderate`). The API connects as a least-privilege user
(`readWrite` on `fraudx` only). The profile writer uses unordered bulk
aggregation-pipeline upserts that are idempotent and order-insensitive:
min/max timestamps, recency lists, and no counters (counts live in Redis and
Cassandra).

| Collection | `_id` | Notable fields / indexes |
|---|---|---|
| `customers` | customer_id | `devices`, `known_ips`, `countries` (50 most recent, deduplicated), `accounts`, `country`, `first_seen`/`last_seen`, `risk_profile{score,level}`; index `(risk_profile.level, risk_profile.score)` |
| `accounts` | account_id | `customer_id` (indexed), `kind` CUSTOMER/MERCHANT |
| `devices` | device_id | first/last seen, metadata |
| `investigations` | transaction_id | risk breakdown, `evidence`, `explanation`, `status`; indexes `(status, created_at)`, `(customer_id, created_at)` |
| `model_registry` | `name:version` | metrics, params, artifact path; partial unique index → one active version per model |

### 5.3 Redis: online feature store (`fraudx.streaming.features`)

Each key family shares one hash tag (`{customer}`, `{device}`, `{ip}`), so
every Lua script touches a single Redis Cluster slot. Every key has a TTL.

| Key | Type | Content | TTL |
|---|---|---|---|
| `fx:cust:{C}:txw` | ZSET | member `tx_id\|amount`, score = event time ms | 2 d |
| `fx:cust:{C}:stats` | HASH | `n`, `sum`, `sumsq` (→ mean/std), `last_ts` | 90 d |
| `fx:cust:{C}:devices` / `:ips` / `:countries` | SET | known values → `is_new_*` | 90 d |
| `fx:cust:{C}:result:{tx}` | STRING | cached raw result of this transaction (idempotency) | 7 d |
| `fx:dev:{D}:txw`, `fx:ip:{IP}:txw` | ZSET | transaction ids by event time (1 h velocity) | 2 d |
| `fx:dev:{D}:custs`, `fx:ip:{IP}:custs` | SET | distinct customers seen on the device / IP | 30 d |
| `fx:cust:{C}:risk` | HASH | latest score and level (Phase 8) | 90 d |

Per event: **one pipelined round trip** of three Lua scripts (customer,
device, IP). The customer script reads the profile *before* the event,
inserts the event, computes the windows, updates the profile and caches its
result atomically. A redelivered transaction hits the cache and returns
identical features without touching the counters. Device/IP scripts only use
idempotent operations.

Windows are evaluated at the transaction's **event time** and include only
transactions up to that instant. Replays, backfills and late events therefore
produce the same values as live traffic.

**Features (`TransactionFeatures`, `FEATURE_VERSION = 1`).** Window
values include the current transaction; profile values describe the
customer before it.

| Feature | Definition |
|---|---|
| `cust_txn_count_{5m,1h,24h}`, `cust_amount_sum_{5m,1h,24h}` | customer velocity and spend in event-time windows |
| `cust_history_count`, `cust_amount_mean`, `cust_amount_std` | profile before this transaction |
| `amount_zscore`, `amount_to_mean_ratio` | deviation from the customer's usual amount |
| `seconds_since_last_txn` | gap to the latest earlier transaction (null for the first or a late one) |
| `is_new_device`, `is_new_ip`, `is_new_country` | value never seen for this customer before |
| `device_txn_count_1h`, `device_customer_count` | device velocity; distinct customers on the device (30 d) |
| `ip_txn_count_1h`, `ip_customer_count` | same for the IP |
| `balance_drain_ratio` | amount / source balance for debits |
| `hour_of_day` | UTC hour |

Measured on the default dataset: one worker processes about 2,000 events/s
(≈0.5 ms per event including the Redis round trip and produce). Scaling is
horizontal, up to one worker per partition (6).

**Cost model:** window sums iterate the customer's last 24 h of transactions,
so per-event cost grows with customer activity. About 60 µs per Lua call on
synthetic data; about 450 µs on IBM AML, whose corporate entities make
thousands of transfers a day. Bounded-cost alternative if needed: hierarchical
time buckets (minute buckets for 5 m/1 h, hour buckets for 24 h, ≤ ~90
operations per event), with approximate window edges.

### 5.4 Neo4j (`infra/neo4j/schema.cypher`)

```text
(:Customer)-[:OWNS]->(:Account)
(:Customer)-[:USES {first_seen,last_seen,last_tx,count}]->(:Device)
(:Device)-[:CONNECTED_FROM {first_seen,last_seen}]->(:IP)
(:IP)-[:LOCATED_IN]->(:Location)
(:Account)-[:TRANSFERRED_TO {count,total_amount,first_seen,last_seen,last_tx}]->(:Account)
```

Transactions are **aggregated onto edges**, not stored as nodes (ADR-004);
the graph grows with entities, not events. Merchants are `Account` nodes with
`kind = "MERCHANT"`. `fraud_flag` on Customer/Account (from labels or analyst
confirmation) drives `fraud_neighbor_count` and `distance_to_known_fraud`.
Uniqueness constraints on every id back the `MERGE` writes. The graph
writer runs three `UNWIND` statements per batch in one transaction. Edge
counters only advance when the event comes after the last counted one in
`(event_time, transaction_id)` order (`last_seen`, `last_tx`). Counted edges
always include the event's own customer or account, whose events arrive in
order on one partition, so redelivery never double-counts, and distinct
transactions with the same timestamp are all counted (IBM AML has minute
resolution).
`CASH_IN` edges point from the agent/merchant to the customer (direction of
money). Merchant accounts are high-degree hubs; graph features should
exclude `kind = "MERCHANT"` from traversals.

## 6. Scoring

```text
sᵢ    = calibrationᵢ(detectorᵢ(x))                  P(fraud | score), fitted on validation
score = round_half_up(100 × Σ wᵢ·sᵢ / Σ wᵢ)   over the available signals i ∈ {ml, anomaly, graph}
score = max(score, floor of every rule hit)     takeover_pattern → high, burst_on_shared_device → medium
level = HIGH if score ≥ high, MEDIUM if ≥ medium, else LOW     (config/risk.yaml)
```

* Every signal is a **calibrated fraud probability** (isotonic regression on
  validation, stored with each model, `fraudx.scoring.detectors`). Without
  calibration the spec's weighted average is worse than the ML model alone
  (test PR-AUC 0.717 vs 0.747): the anomaly percentile (≈ 0.5 on normal
  traffic) drowns the other signals.
* Weights and thresholds come from `config/risk.yaml`, validated at load
  (weights sum to 1, thresholds ordered), and are **chosen on validation** by
  `make tune-risk` (ADR-030): ml 0.5, anomaly 0.1, graph 0.4; MEDIUM ≥ 8
  (best F2, review queue), HIGH = alert ≥ 23 (best F1). Evidence:
  `docs/experiments/2026-10-16-risk-engine.md` (test PR-AUC 0.851 vs 0.747 for
  ML alone; holdout alert precision 0.79, recall 0.88).
* A missing signal (model not loaded, graph timeout) has its weight
  redistributed and is listed in `missing_signals`. It is never counted as 0.
* Deterministic rules (`fraudx.scoring.rules`) set a minimum level; only
  rules with ≥ 0.85 precision on training and holdout data are kept.
* ML training is offline (`ml` scripts → `models/` + `model_registry`); the
  scorer only loads a registered, versioned artifact.
* Evaluation uses precision, recall, F1, ROC-AUC and **PR-AUC (primary)** with a
  time-based split. Accuracy is not used because of class imbalance.

### 6.4 Scorer (`fraudx.scoring`)

* Consumes `transactions.features` in batches of 500: vectorises the
  transactions, reads their graph features from Neo4j in one query, scores
  the three calibrated signals, evaluates the rules and calls the risk
  engine. Publishes every `ScoredTransaction` (event, features, graph
  features, risk, model versions) to `transactions.scored`, alerts to
  `fraud.alerts`, and the customer's latest risk to Redis.
* **Degradation:** a Neo4j error scores the batch without the graph signal
  (`missing_signals`, metric `fraudx_signal_missing_total`) rather than
  stopping. With no model at all the batch is retried (visible as lag).
* **Model updates:** the active versions in `model_registry` are checked every
  minute and reloaded when they change; artifacts and `config/risk.yaml` are
  mounted read-only, so retraining or re-tuning needs no image rebuild
  (restart the scorer for a new config).
* **Live = offline:** replaying the demo dataset through the running stack
  gives alert precision 0.790 / recall 0.879, against 0.792 / 0.876 in the
  offline holdout evaluation.
* Metrics: `fraudx_scored_total{level}`, `fraudx_alerts_total`,
  `fraudx_risk_score`, `fraudx_rule_hits_total{rule}`.

### 6.1 Supervised model (`fraudx.ml`)

```text
dataset dir ──build-features──► features_v1.npz ──train──► models/fraud_lgbm/<version>/
  (events.jsonl +                (same Lua/Python feature     model.txt + metadata.json
   ground_truth.csv)              code as the live processor,  ──register──► MongoDB model_registry
                                  disposable redis-offline)      (one active version per model)
```

* **No training/serving skew:** training features come from
  `OnlineFeatureStore.compute_many` (the live code path), and model input from
  `fraudx.ml.vectorize` (shared with the scorer). `FraudModel.load` refuses an
  artifact whose `feature_version` or feature names differ from the pipeline.
* **Data:** trained on a separate dataset (`make data-train`: seed 1337, 5,000
  customers, 30 days, 12 rings). The demo dataset (seed 42) is an
  out-of-sample holdout, so the demo stream is never training data.
* **Protocol:** 70/15/15 split by event time; candidates unweighted vs
  `scale_pos_weight = sqrt(neg/pos)`, early-stopped on validation PR-AUC;
  threshold = max F1 on validation; test and holdout metrics include per-scenario
  recall / false-positive rate, Brier score, and ablations without the
  balance and network feature groups. PR-AUC and precision are also reported
  at the dataset's overall fraud rate (positives reweighted), because they
  depend on the base rate and the time split gives each set a different one.
  Each run writes a report to `docs/experiments/`; `python -m fraudx.ml
  register <artifact>` records a saved model from its own `metadata.json`.
* **Version** = `<UTC timestamp>-<sha256(features file)[:8]>`, which ties a
  model to its exact training data.

### 6.2 Anomaly detector (`fraudx.ml.anomaly`)

* **Isolation Forest** (200 trees, 256 samples each) fitted on the
  **legitimate** rows of the training split only: it learns normal traffic
  and needs no labels to score. Same feature matrix and time split as §6.1,
  numeric features only (categorical codes have no distance); missing values
  are filled with training medians stored in the artifact.
* **Calibrated signal:** the raw score becomes its percentile among
  legitimate validation traffic, so 0.99 means "more unusual than 99 % of
  normal transactions" on every dataset. This is the risk engine's
  `anomaly` signal.
* Evaluated with the same report as §6.1 at threshold 0.99. "Unusual" is not
  "fraudulent": it catches blunt fraud (takeovers, cash-outs, card testing)
  and also flags legitimate outliers (large purchases, bursts), so its value
  is measured as a contribution to the combined score (Phase 8).
* `python -m fraudx.ml train-anomaly` (part of `make train`); artifacts in
  `models/anomaly_iforest/<version>/`, registered like §6.1.

### 6.3 Graph features and graph model (`fraudx.storage.graph_features`)

Six features per transaction, read from Neo4j in money-flow terms (payer →
payee) and **as of the event time**: only edges whose `first_seen` is
earlier than the transaction count, so a transaction never sees itself and
an offline replay gives the live values.

| Feature | Pattern |
|---|---|
| `payee_payer_count` | other accounts that paid the payee (mule, fan-in) |
| `payee_payee_count` | accounts the payee paid on (layering) |
| `payer_payee_count` | other accounts the payer paid (fan-out) |
| `is_new_payee` | first payment from this payer to this payee |
| `customer_ring_size` | other customers sharing a device with the customer |
| `payee_owner_ring_size` | customers sharing a device with the payee's owner (ring mule) |

* One `UNWIND` query per batch, every pattern anchored on the indexed ids of
  the transaction's customer and accounts.
* **Training without skew:** `build-graph-features` replays the dataset in
  event-time order into an empty, disposable `neo4j-offline` (profile `jobs`,
  RAM-backed). For each group of events (at most one minute of event time,
  close to live writer lag) it reads features with the same query, then
  writes the group with the production `GraphWriter`.
* **Graph signal:** `train --graph` fits LightGBM on the graph features only
  (`graph_lgbm`, same protocol and report as §6.1). Its probability is the
  risk engine's `graph` signal; the transaction-feature model stays the `ml`
  signal, so each signal can be explained and weighted on its own.

## 7. Alerts, investigations and explanations

```text
fraud.alerts ─► alert-handler ─► evidence (alert message + MongoDB profile)
                               ─► explanation: template (default) | Claude (opt-in)
                               ─► grounding check ─► MongoDB investigations (OPEN)
                               ─► optional webhook (Slack/Teams/...)
```

* Explanations are generated only for alerts (score ≥ alert threshold),
  once per alert: the investigation is created with `$setOnInsert`, so a
  redelivered alert neither resets an analyst's decision nor pays for a
  second explanation.
* **Evidence** (`fraudx.explain.evidence`): flat, citable keys built from the
  alert itself (it carries every input of the decision: signals and their
  share of the score, rule hits, transaction and graph features, model
  versions) and the customer's profile, e.g. `graph.payee_owner_ring_size`,
  `features.balance_drain_ratio`, `contribution.ml`, `payee.kind`.
* **Template** (`fraudx.explain.template`, default, offline, deterministic):
  12 reason rules ordered strongest first (rule hits, graph patterns, then
  amount, balance, velocity, new device/country/IP, shared device, the
  strongest model signal); at most five reasons, each citing its keys.
* **Claude** (`EXPLAIN_PROVIDER=anthropic`, `fraudx.explain.llm`):
  `claude-opus-5-5`, JSON constrained by a schema (`output_config.format`),
  effort `low`, server-side refusal fallback (`fallbacks: "default"`). It
  receives the pseudonymised evidence only: identifiers removed, and the
  evidence never contains IPs or raw balances. API errors, refusals, or an
  answer with no grounded reason fall back to the template.
* **Grounding:** every reason, template or LLM, must cite keys that exist in
  the evidence, or it is dropped (`dropped_ungrounded` is stored).
* The API serves the analyst queue (`GET /fraud/investigations`), one case
  (`GET /fraud/{tx}`), the explanation with each reason's cited values
  (`GET /fraud/{tx}/explanation`) and the decision (`PATCH /fraud/{tx}`).

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
* Pipeline metrics from every worker (`fraudx.messaging.consumer`): messages
  by outcome (ok / dlq / retry), handler time, stage latency (produce →
  processed), consumer lag per partition (from librdkafka statistics, no
  kafka-exporter). The scorer adds scores by level, the score distribution,
  alerts, rule hits and missing signals; the alert handler counts
  investigations and webhook failures.
* **Grafana** dashboard *FraudX pipeline*, provisioned from
  `infra/grafana/provisioning/dashboards/`: pipeline health (throughput,
  lag, dead-lettered, degraded scoring), latency (p95 per stage and per
  message), risk (medium/high per minute in the status colors, rule hits,
  investigations) and API / Kafka panels. `make up` reloads Prometheus.
* **Measured** (`make bench-stream`, `docs/benchmarks/streaming.md`):
  end-to-end latency from `transactions.raw` to a scored decision, and the
  pipeline's sustained throughput on one host. Batch workers wait at most
  50 ms for a batch to fill; the earlier 1 s wait put the steady-state p50
  at 1.3 s.
* JSON logs on stdout with contextual fields.
* `/health` is liveness (no dependencies); `/health/ready` checks all five
  backends and the presence of the Kafka topics.

## 10. Scalability path

| Concern | Local | Scale-out |
|---|---|---|
| Kafka | 1 broker, 6 partitions | 3+ brokers, RF=3; up to 6 consumers per group without re-partitioning |
| Stream processing | one replica per worker; the whole pipeline (features, graph query, three models) sustains about 1k tx/s on one host (`make bench-stream`) | `--scale feature-processor=6 --scale scorer=6`; state lives in Redis and Neo4j, so workers are stateless |
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

`python -m fraudx.ingestion generate` (or `make data`) writes a synthetic
dataset directory. Public datasets use `python -m fraudx.datasets paysim|ibm_aml`
(or `make paysim` / `make ibm-aml`, staging profile → clean → canonicalize →
features → train). Both produce the same layout:

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
| `app_scam` | fraud | **v2.** Authorised push payment: the victim's own device and IP, home country, daytime, transfer 3–12× usual to a ring mule. Only the payee is suspicious |
| `account_takeover_local` | fraud | **v2.** New device but home country and normal hours; 40–70 % drain to a mule |
| `mule_slow_drip` | fraud | **v2.** The mule CASH_OUTs 20–45 % in 2–4 chunks, 1–18 h apart, from their personal device |
| `travel` | legit | days abroad with foreign IPs/locations |
| `new_device` | legit | a new phone that becomes a known device |
| `large_purchase` | legit | an occasional purchase 10–30× the usual amount |
| `legit_burst` | legit | **v2.** 3–8 quick small purchases (looks like card testing) |
| `new_payee` | legit | **v2.** transfer 3–12× usual to a non-contact (looks exactly like `app_scam`) |
| (shared devices) | legit | ~2 % of customers share a device with family |

Incident mix (weights): takeover 0.25, card testing 0.20, APP scam 0.35, local
takeover 0.20. Set the v2 weights to 0 for v1-style data. Defaults (2,000
customers, 14 days) give 65,292 events at 0.41 % fraud in about 2 s.

**Public datasets** (`fraudx.datasets`, ADR-024/025). PaySim and IBM AML
HI-Small go through explicit stages: profile → clean (named rules recording
rationale, rows and labels affected) → canonicalize (to `TransactionEvent`) →
features (online code on the disposable `redis-offline`, ADR-026) → train.
Each dataset gets a generated report in `docs/data/`. Fields a source lacks
stay empty; nothing is invented. See [docs/data/README.md](docs/data/README.md).

## 13. Dashboard (`frontend/`)

React + TypeScript built with Vite; nginx serves it and proxies `/api` to the
API (same origin, no CORS; buffering off for the SSE stream). Pages follow the
spec:

| Page | Data |
|---|---|
| Overview | KPIs from `GET /stats/overview`: scored, tx/s and its 30-min sparkline, levels, average score (Prometheus, last 24 h); confirmed fraud and **analyst-confirmed precision** (MongoDB decisions: the live stream carries no labels, ADR-019); the day's alert feed (Cassandra) |
| Live stream | `GET /stream/scored` (SSE): each viewer reads `transactions.scored` from its end with a throw-away consumer group, so viewers never affect the pipeline |
| Investigations | queue (MongoDB), case in the spec's order: transaction → risk score → ML / anomaly / graph signals → graph relationships → explanation with each reason's cited evidence → analyst decision |
| Graph | a customer's devices, the customers sharing them, and money counterparties (Neo4j), with the device and counterparty tables |

Charts follow one method: risk levels use the fixed status palette with an
icon and a label (never color alone); node kinds use three categorical slots
validated for color-vision deficiency in both themes; light and dark are
separate token sets, with a manual override.
