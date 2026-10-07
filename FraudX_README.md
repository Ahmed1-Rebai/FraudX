# FraudX --- AI-Powered Polyglot NoSQL Platform

> **Specification status (2026-10-07).** This is the original project
> specification, kept for reference. The engineering review revised several
> points. **[ARCHITECTURE.md](ARCHITECTURE.md) is the authoritative
> architecture**, and every change is justified as an ADR in
> [memory.md](memory.md#architectural-decisions). Main revisions:
>
> | Spec | Revised | ADR |
> |---|---|---|
> | Apache Flink for stream processing | Python consumer-group workers with Redis sliding windows (Flink deferred) | 001 |
> | Debezium, Spark, Kubernetes, Terraform (architecture image) | Out of MVP scope | 002 |
> | Topics `.enriched`, `.features`, `fraud.predictions` | `transactions.features`, `transactions.scored`, plus `transactions.dlq` | 003 |
> | `Transaction` nodes in Neo4j | Entities + aggregated `TRANSFERRED_TO` edges | 004 |
> | One Cassandra table, `double` amounts | Three query tables, `decimal` amounts | 005 |
> | Missing detector counted as 0, no place for rules | Reweight over available signals; rules set a score floor | 008 |
> | Top-level `ingestion/`, `ml/`, `api/`... folders | Single `src/fraudx` package, one image | 011 |
> | PaySim download required | Built-in synthetic generator by default, PaySim optional | 013 |

> **Advanced Data Engineering + NoSQL + Real-Time Streaming + AI
> project**

FraudX is a distributed platform designed to detect, analyze, score, and
explain potentially fraudulent financial transactions in real time.

The project is intentionally designed as a **Data Engineering / NoSQL
project**, not as a simple CRUD application. Each technology must solve
a specific data or architectural problem.

------------------------------------------------------------------------

## 1. Project Vision

FraudX receives a continuous stream of financial transactions and
contextual information such as:

-   customer/account information
-   transaction amount and type
-   device information
-   IP/location
-   transaction history
-   relationships between accounts, devices and IP addresses

The platform processes these events in real time, stores them using
different NoSQL models, extracts features, applies multiple AI detection
strategies, calculates a global risk score, and provides an explanation
for security analysts.

### Main objective

> Detect suspicious transactions in real time using a combination of
> **polyglot NoSQL storage, stream processing, machine learning, anomaly
> detection, graph analysis, and explainable AI**.

------------------------------------------------------------------------

# 2. What Makes This Project Advanced?

FraudX combines several important Data Engineering concepts:

-   Event-driven architecture
-   Real-time data ingestion
-   Stream processing
-   Batch processing
-   Polyglot persistence
-   Document databases
-   Wide-column databases
-   Key-value databases
-   Graph databases
-   Feature engineering
-   Supervised machine learning
-   Anomaly detection
-   Graph-based fraud analysis
-   Explainable AI
-   REST APIs
-   Monitoring and observability
-   Containerization
-   Horizontal scalability

The goal is not to use many technologies for the sake of using them.

Each technology has a clearly defined responsibility.

------------------------------------------------------------------------

# 3. High-Level Architecture

``` text
                         ┌──────────────────────┐
                         │   FINANCIAL DATA     │
                         │                      │
                         │ PaySim / Synthetic   │
                         │ Fraud Events         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │       KAFKA          │
                         │   Event Streaming    │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │       FLINK          │
                         │ Stream Processing    │
                         │ Feature Extraction  │
                         └──────────┬───────────┘
                                    │
                  ┌─────────────────┼─────────────────┐
                  │                 │                 │
                  ▼                 ▼                 ▼
           ┌────────────┐    ┌────────────┐    ┌────────────┐
           │  MongoDB   │    │ Cassandra  │    │   Redis    │
           │ Documents  │    │   Events   │    │ Real-Time  │
           │ Profiles   │    │  History   │    │  Features  │
           └─────┬──────┘    └─────┬──────┘    └─────┬──────┘
                 │                 │                 │
                 └─────────────────┼─────────────────┘
                                   │
                                   ▼
                            ┌──────────────┐
                            │    Neo4j     │
                            │ Fraud Graph  │
                            │ Relationships│
                            └──────┬───────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
              ┌──────────┐  ┌────────────┐  ┌───────────┐
              │ ML Model │  │  Anomaly   │  │   Graph   │
              │ XGBoost  │  │ Detection  │  │   Risk    │
              │ LightGBM │  │ Isolation  │  │ Analysis  │
              │          │  │  Forest    │  │           │
              └────┬─────┘  └─────┬──────┘  └─────┬─────┘
                   │              │                │
                   └──────────────┼────────────────┘
                                  ▼
                       ┌─────────────────────┐
                       │    RISK ENGINE      │
                       │                     │
                       │ ML Risk       40%   │
                       │ Anomaly Risk  30%   │
                       │ Graph Risk    30%   │
                       └──────────┬──────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │   RISK SCORE     │
                         │      0–100       │
                         └────────┬─────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
             ┌──────────────┐           ┌──────────────┐
             │ LLM Analyst  │           │ Alert Engine │
             │ Explanation  │           │              │
             └──────┬───────┘           └──────────────┘
                    │
                    ▼
             ┌────────────────┐
             │ FastAPI + React│
             │   Dashboard    │
             └────────────────┘
```

------------------------------------------------------------------------

# 4. Technology Stack

## Data Source

### PaySim

Use the PaySim synthetic financial transaction dataset as the initial
dataset.

It provides:

-   transaction type
-   amount
-   source account
-   destination account
-   source balance
-   destination balance
-   fraud label

The dataset is synthetic and is therefore suitable for academic
experimentation.

A synthetic event generator should later enrich transactions with:

-   device IDs
-   IP addresses
-   locations
-   merchant IDs
-   sessions
-   timestamps
-   account/device relationships

------------------------------------------------------------------------

# 5. Technology Responsibilities

## Apache Kafka --- Event Ingestion

Kafka is the central event backbone.

Responsibilities:

-   ingest transactions
-   decouple producers from consumers
-   provide replayable events
-   distribute events to processing services
-   simulate high-throughput transaction streams

Suggested topics:

``` text
transactions.raw
transactions.enriched
transactions.features
fraud.predictions
fraud.alerts
```

> **Revised (ADR-003):** `transactions.raw`, `transactions.features`,
> `transactions.scored`, `transactions.dlq`, `fraud.alerts`, keyed by
> `customer_id`. See ARCHITECTURE.md §4.

Example event:

``` json
{
  "transaction_id": "TX-100023",
  "timestamp": "2026-10-07T10:15:32Z",
  "customer_id": "C-1002",
  "source_account": "A-1002",
  "destination_account": "A-9821",
  "amount": 8500.50,
  "transaction_type": "TRANSFER",
  "device_id": "DEV-88",
  "ip_address": "192.168.1.20",
  "location": {
    "country": "TN",
    "city": "Sfax"
  }
}
```

------------------------------------------------------------------------

# 6. Apache Flink --- Real-Time Processing

> **Revised (ADR-001):** the MVP implements these responsibilities with
> Python consumer-group workers that keep sliding-window state in Redis
> (event time, idempotent). Flink is a documented future option for
> requirements beyond that design.

Flink processes incoming Kafka events.

Responsibilities:

-   validation
-   normalization
-   enrichment
-   windowed aggregations
-   velocity calculations
-   real-time feature extraction
-   anomaly-related features
-   routing events to storage and AI services

Example features:

``` text
transactions_last_5min
transactions_last_1hour
amount_last_1hour
average_transaction_amount
amount_deviation
new_device
new_ip
new_location
```

Example:

``` text
User normally performs:

5 transactions/day
average amount = 80 TND

Current behavior:

12 transactions in 5 minutes
amount = 4,500 TND

=> suspicious behavior
```

------------------------------------------------------------------------

# 7. MongoDB --- Document Database

MongoDB stores flexible, semi-structured entities.

Use it for:

### Customer profiles

``` json
{
  "customer_id": "C-1002",
  "name": "Customer",
  "country": "TN",
  "devices": ["DEV-1", "DEV-8"],
  "known_ips": ["10.0.0.1"],
  "risk_profile": {
    "score": 34,
    "level": "LOW"
  }
}
```

### Use MongoDB for

-   customer profiles
-   account metadata
-   device metadata
-   fraud investigation results
-   model explanations
-   flexible metadata

MongoDB is **not** the primary store for the entire transaction history.

------------------------------------------------------------------------

# 8. Apache Cassandra --- Massive Transaction History

Cassandra stores high-volume historical events.

The data model must be designed according to query patterns.

Example table:

``` sql
CREATE TABLE transactions_by_customer (
    customer_id text,
    event_date date,
    timestamp timestamp,
    transaction_id text,
    amount double,
    transaction_type text,
    source_account text,
    destination_account text,
    fraud_score double,
    PRIMARY KEY ((customer_id, event_date), timestamp, transaction_id)
);
```

Important principle:

> Design Cassandra tables around the queries the application must
> execute.

Possible queries:

-   Get transactions of a customer for a day
-   Get recent transactions
-   Get transactions within a time window
-   Get historical fraud events

------------------------------------------------------------------------

# 9. Redis --- Real-Time Feature Store / Cache

Redis is used for low-latency access.

Examples:

``` text
customer:C-1002:transaction_count_5m
customer:C-1002:amount_1h
customer:C-1002:last_device
device:DEV-88:transaction_velocity
ip:192.168.1.20:request_count
customer:C-1002:risk_score
```

Redis should provide millisecond-level access to frequently needed
features.

------------------------------------------------------------------------

# 10. Neo4j --- Fraud Relationship Graph

Neo4j models relationships that are difficult to represent efficiently
in relational or document structures.

Nodes:

``` text
Customer
Account
Device
IP
Location
Merchant
Transaction
```

Relationships:

``` text
Customer ──OWNS──> Account
Customer ──USES──> Device
Device ──CONNECTED_FROM──> IP
Transaction ──FROM──> Account
Transaction ──TO──> Account
Transaction ──AT──> Location
```

Example fraud ring:

``` text
Customer A ──USES──> Device X
Customer B ──USES──> Device X
Customer C ──USES──> Device X

Device X ──CONNECTED_FROM──> IP Y
```

This can indicate a potentially suspicious shared infrastructure.

> **Revised (ADR-004):** transactions are aggregated onto
> `(:Account)-[:TRANSFERRED_TO {count, total_amount, ...}]->(:Account)` edges
> instead of `Transaction` nodes; location hangs off `IP`. See
> ARCHITECTURE.md §5.4.

------------------------------------------------------------------------

# 11. AI Layer

The AI layer contains three complementary detection mechanisms.

## 11.1 Supervised Fraud Detection

Use:

-   XGBoost
-   or LightGBM

Input:

``` text
transaction amount
transaction type
velocity features
balance changes
device history
location history
customer behavior
```

Output:

``` text
fraud_probability = 0.91
```

------------------------------------------------------------------------

## 11.2 Anomaly Detection

Use:

-   Isolation Forest initially
-   Autoencoder as an advanced extension

The model learns normal behavior and identifies unusual transactions.

Example:

``` text
Normal:

amount: 20–150 TND
location: Tunisia
device: DEV-1

New event:

amount: 8,500 TND
location: Germany
device: DEV-99

=> high anomaly score
```

------------------------------------------------------------------------

## 11.3 Graph Risk

Extract graph-based features from Neo4j:

``` text
shared_device_count
shared_ip_count
connected_account_count
distance_to_known_fraud
fraud_neighbor_count
transaction_path_length
```

These features are passed to the risk engine.

------------------------------------------------------------------------

# 12. Risk Scoring Engine

The platform combines the three signals.

Initial formula:

``` text
Risk Score =
    0.40 × ML Fraud Probability
  + 0.30 × Anomaly Score
  + 0.30 × Graph Risk
```

Normalize the result to:

``` text
0–100
```

Classification:

``` text
0–29    LOW
30–69   MEDIUM
70–100  HIGH
```

Example:

``` text
ML prediction       = 0.91
Anomaly score       = 0.83
Graph risk          = 0.96

Risk Score =
0.40(0.91)
+ 0.30(0.83)
+ 0.30(0.96)

= 0.901

Final Risk Score = 90/100

Result = HIGH RISK
```

The weights must be configurable, not hard-coded permanently.

> **Revised (ADR-008):** weights live in `config/risk.yaml` (validated). If a
> signal is unavailable its weight is redistributed, not counted as 0, and
> deterministic rules can impose a minimum score.

------------------------------------------------------------------------

# 13. LLM --- AI Fraud Investigator

The LLM does **not** replace the fraud detection model.

Its responsibility is to explain the decision.

Input:

``` text
Transaction
Risk score
ML prediction
Anomaly score
Graph findings
Customer history
Relevant evidence
```

Output:

``` text
HIGH RISK TRANSACTION

Reasons:
1. Transaction amount is significantly above the customer's normal behavior.
2. A new device was detected.
3. The device is associated with multiple accounts.
4. The transaction occurred from an unusual location.
5. Connected accounts have previous suspicious activity.
```

The explanation must always be based on retrieved evidence.

Do not allow the LLM to invent evidence.

Recommended approach:

``` text
Retrieval
   ↓
Evidence aggregation
   ↓
Prompt construction
   ↓
LLM
   ↓
Structured explanation
```

------------------------------------------------------------------------

# 14. API Layer

Use **FastAPI**.

Main endpoints:

``` text
GET  /health
GET  /transactions
GET  /transactions/{id}
GET  /customers/{id}
GET  /customers/{id}/risk
GET  /fraud/alerts
GET  /fraud/{transaction_id}
GET  /fraud/{transaction_id}/explanation
POST /transactions/simulate
POST /models/predict
```

The API must aggregate data from the appropriate storage systems.

------------------------------------------------------------------------

# 15. Frontend

Use:

-   React
-   TypeScript
-   modern dashboard UI

Main pages:

### Dashboard

Display:

``` text
Total transactions
Transactions/sec
Fraud detected
High-risk transactions
Average risk score
Model accuracy
```

### Live Transaction Stream

Display incoming events in real time.

### Fraud Investigation

For a selected transaction:

``` text
Transaction details
       ↓
Risk score
       ↓
ML prediction
       ↓
Anomaly score
       ↓
Graph relationships
       ↓
AI explanation
```

### Graph Investigation

Visualize:

``` text
Customer
   ↓
Account
   ↓
Transaction
   ↓
Device
   ↓
IP
```

------------------------------------------------------------------------

# 16. Alert System

When:

``` text
risk_score >= 70
```

generate an alert.

Possible integrations:

``` text
Email
Slack
Webhook
```

Alert example:

``` text
🚨 HIGH RISK TRANSACTION

Transaction: TX-100023
Customer: C-1002
Amount: 8500.50 TND
Risk Score: 90/100

Primary reasons:
- abnormal amount
- new device
- suspicious graph connections
```

------------------------------------------------------------------------

# 17. Project Structure

Recommended repository:

> **Revised (ADR-011):** Python code lives in one package `src/fraudx/`
> (`core`, `ingestion`, `streaming`, `storage`, `scoring`, `explain`,
> `alerts`, `api`) with provisioning in `infra/`. See README.md.

``` text
fraudx/
│
├── README.md
├── docker-compose.yml
├── .env.example
├── Makefile
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── generated/
│
├── ingestion/
│   ├── kafka_producer/
│   └── synthetic_generator/
│
├── streaming/
│   └── flink/
│
├── storage/
│   ├── mongodb/
│   ├── cassandra/
│   ├── redis/
│   └── neo4j/
│
├── ml/
│   ├── fraud_detection/
│   ├── anomaly_detection/
│   ├── graph_features/
│   └── training/
│
├── risk_engine/
│
├── llm/
│   ├── prompts/
│   ├── retrieval/
│   └── explanation/
│
├── api/
│   └── fastapi/
│
├── frontend/
│   └── react/
│
├── monitoring/
│   ├── prometheus/
│   └── grafana/
│
└── docs/
    ├── architecture/
    ├── data-model/
    ├── api/
    └── experiments/
```

------------------------------------------------------------------------

# 18. Local Development Environment

The first version must run locally using Docker Compose.

Required services:

``` text
Kafka
Kafka UI
Flink
MongoDB
Cassandra
Redis
Neo4j
FastAPI
React
Prometheus
Grafana
```

Do **not** require Kubernetes for the first milestone.

> **Implemented:** all services above except Flink (ADR-001) and React
> (Phase 10), plus one-shot provisioning jobs for topics and schemas.

Kubernetes can be added later as an advanced deployment phase.

------------------------------------------------------------------------

# 19. Development Phases

## Phase 1 --- Data

-   Download PaySim
-   Validate dataset
-   Clean and normalize records
-   Create a common transaction schema
-   Build synthetic enrichment generator

Deliverable:

``` text
enriched_transactions.json
```

> **Implemented (ADR-013, ADR-017):** `make data` writes
> `data/generated/enriched_transactions.jsonl` plus `ground_truth.csv` and
> `summary.json`; `make data-paysim` does the same for a PaySim CSV.

------------------------------------------------------------------------

## Phase 2 --- Kafka

-   Start Kafka
-   Create topics
-   Implement producer
-   Stream transactions
-   Implement consumer

Deliverable:

``` text
PaySim → Kafka → Consumer
```

> **Implemented:** `make replay` (or `docker compose run --rm replay`)
> publishes a dataset to `transactions.raw`; `fraudx.messaging.StreamWorker`
> is the consumer runtime for every stage (ARCHITECTURE.md §4.1).

------------------------------------------------------------------------

## Phase 3 --- NoSQL Storage

Implement:

``` text
Kafka
  ├── MongoDB
  ├── Cassandra
  ├── Redis
  └── Neo4j
```

Deliverable:

All four databases receive appropriate data.

> **Implemented (ADR-021):** the feature processor fills Redis; the
> `graph-writer`, `history-writer` and `profile-writer` services fill Neo4j,
> Cassandra and MongoDB from `transactions.features`.

------------------------------------------------------------------------

## Phase 4 --- Stream Processing

> **Revised:** stream processing becomes Phase 3 (before the storage sinks)
> and uses Python workers + Redis (ADR-001). See memory.md for the current
> phase plan.

Add Flink.

Implement:

-   event validation
-   window aggregation
-   velocity features
-   real-time enrichment

Deliverable:

``` text
Kafka → Flink → Feature streams
```

------------------------------------------------------------------------

## Phase 5 --- Machine Learning

Train:

``` text
XGBoost / LightGBM
```

Evaluate:

-   Precision
-   Recall
-   F1-score
-   ROC-AUC
-   PR-AUC

Because fraud is highly imbalanced, **do not rely only on accuracy**.

------------------------------------------------------------------------

## Phase 6 --- Anomaly Detection

Implement:

``` text
Isolation Forest
```

Generate anomaly scores.

------------------------------------------------------------------------

## Phase 7 --- Graph Analysis

Build Neo4j relationships.

Implement graph-based fraud features.

Optional advanced extension:

``` text
Graph Neural Network
```

Do not make GNN mandatory for the first version.

------------------------------------------------------------------------

## Phase 8 --- Risk Engine

Combine:

``` text
ML
+
Anomaly
+
Graph
```

into one configurable risk score.

------------------------------------------------------------------------

## Phase 9 --- LLM Investigator

Implement evidence retrieval.

Generate explanations from:

``` text
MongoDB
Cassandra
Redis
Neo4j
ML outputs
```

The LLM must explain evidence, not invent facts.

------------------------------------------------------------------------

## Phase 10 --- Dashboard

Implement:

-   live transactions
-   fraud alerts
-   risk scores
-   customer investigation
-   graph visualization
-   AI explanations
-   system metrics

------------------------------------------------------------------------

## Phase 11 --- Benchmarking

Compare:

### Storage

``` text
MongoDB vs Cassandra
```

Measure:

-   write latency
-   read latency
-   throughput
-   storage size

### Cache

``` text
MongoDB vs Redis
```

Measure:

-   response latency
-   throughput

### Streaming

Measure:

``` text
events/sec
processing latency
end-to-end latency
```

------------------------------------------------------------------------

# 20. Observability

Use Prometheus + Grafana.

Track:

``` text
Kafka throughput
Kafka consumer lag
Flink processing latency
MongoDB operations
Cassandra latency
Redis latency
API latency
transactions/sec
fraud detection rate
model inference latency
```

The dashboard should allow the supervisor to see that this is a real
Data Engineering platform rather than a static ML notebook.

------------------------------------------------------------------------

# 21. Important Design Principles

## Do not use one database for everything

The purpose of the project is to demonstrate **polyglot persistence**.

Each database has a specific role.

## Do not send every event directly to the LLM

The LLM is expensive and unnecessary for every transaction.

Use the LLM primarily for:

-   high-risk transactions
-   analyst investigation
-   explanations
-   summaries

## Do not use ML as the only fraud mechanism

FraudX intentionally combines:

``` text
Rules
+
Supervised ML
+
Anomaly Detection
+
Graph Analysis
```

## Do not use Kafka only as decoration

Kafka must genuinely decouple ingestion from downstream processing.

## Do not hard-code risk weights

Use configuration:

``` yaml
risk:
  ml_weight: 0.40
  anomaly_weight: 0.30
  graph_weight: 0.30
  high_threshold: 70
```

------------------------------------------------------------------------

# 22. Success Criteria

The project is considered successful when the following pipeline works
end-to-end:

``` text
PaySim / Synthetic Data
        ↓
      Kafka
        ↓
      Flink
        ↓
 ┌──────┼────────┬────────┐
 ↓      ↓        ↓        ↓
Mongo Cassandra Redis    Neo4j
 └──────┼────────┬────────┘
        ↓
   Feature Engineering
        ↓
 ┌──────┼─────────────┐
 ↓      ↓             ↓
ML   Anomaly       Graph
 └──────┼─────────────┘
        ↓
   Risk Engine
        ↓
   Risk Score 0–100
        ↓
 ┌──────┴───────────┐
 ↓                  ↓
Alert          LLM Explanation
 ↓                  ↓
 └────────┬─────────┘
          ↓
    React Dashboard
```

A complete demonstration should be able to take a transaction from
ingestion to a final fraud decision in near real time.

------------------------------------------------------------------------

# 23. Final Academic Positioning

### Project title

**FraudX: Design and Implementation of an AI-Powered Polyglot NoSQL
Platform for Real-Time Fraud Detection and Risk Intelligence**

### Main domains

``` text
Data Engineering
NoSQL
Distributed Systems
Real-Time Streaming
Machine Learning
Graph Analytics
Explainable AI
Cloud-Native Architecture
```

### Core research/engineering question

> How can heterogeneous NoSQL databases, real-time stream processing,
> graph analytics, and AI models be combined to detect and explain
> financial fraud with low latency?

------------------------------------------------------------------------


# 25. Coding Agent Instructions

The coding agent must:

1.  Build the system incrementally.
2.  Keep every service independently runnable.
3.  Use Docker Compose for local infrastructure.
4.  Provide `.env.example`.
5.  Never hard-code credentials.
6.  Provide health checks for every service.
7.  Add unit tests for business logic.
8.  Add integration tests for Kafka and databases.
9.  Document every database schema.
10. Explain why each NoSQL database is used.
11. Keep ML training separate from inference.
12. Version trained models.
13. Make risk weights configurable.
14. Log processing latency and errors.
15. Never send sensitive/raw transaction data unnecessarily to the LLM.
16. Never let the LLM fabricate fraud evidence.
17. Provide reproducible commands to start the complete platform.
18. Provide seed/demo data so the system can be demonstrated without
    external dependencies.
19. Keep the architecture modular so individual components can later be
    replaced.
20. Prioritize a working end-to-end MVP before advanced features.

------------------------------------------------------------------------

# 26. Definition of Done

The project is ready for academic demonstration when a supervisor can
run:

``` bash
docker compose up -d
```

then start the data pipeline and observe:

-   transactions flowing through every Kafka topic (Kafka UI)
-   the four databases being populated with their respective data
-   risk scores and alerts produced in near real time
-   an evidence-based explanation for each high-risk transaction
-   live transactions, alerts and investigations in the dashboard
-   throughput, latency and consumer lag in Grafana

all from a reproducible setup that needs no external services.