# FraudX

**AI-powered polyglot NoSQL platform for real-time fraud detection and risk intelligence.**

FraudX streams financial transactions through Kafka, computes behavioural
features online, scores every transaction with rules, supervised ML, anomaly
detection and graph analysis, and explains high-risk decisions with evidence
retrieved from four purpose-built NoSQL stores.

| Store | Role |
|---|---|
| **Redis** | Online feature store (sliding windows, known devices/IPs), latest risk |
| **Cassandra** | High-volume transaction history and risk event feed |
| **MongoDB** | Customer/account/device profiles, investigations, model registry |
| **Neo4j** | Customer–account–device–IP relationship graph (fraud rings) |

- Architecture: [ARCHITECTURE.md](ARCHITECTURE.md)
- Engineering memory (decisions, progress, conventions): [memory.md](memory.md)
- Original specification: [FraudX_README.md](FraudX_README.md)

> **Status:** All phases (0–11) are complete: replayed transactions flow through Kafka, get online
> features in Redis, and land in Cassandra, MongoDB and Neo4j, which the API reads back.
> A versioned LightGBM model, an Isolation Forest anomaly detector and a graph model are
> trained and registered, on the synthetic data and on PaySim and IBM AML HI-Small (reports in
> [`docs/data/`](docs/data/README.md) and [`docs/experiments/`](docs/experiments/)).
> Storage benchmarks are in [`docs/benchmarks/`](docs/benchmarks/storage.md).
> Graph features (Phase 7) feed a third, graph-based model. The scorer (Phase 8) combines
> the three calibrated signals and two rules into a 0–100 risk score for every transaction
> and raises alerts (live: precision 0.79, recall 0.88 on the demo data).
> Every alert opens an investigation with an evidence-grounded explanation (Phase 9).
> The React dashboard (Phase 10) shows KPIs, the live stream, investigations and the graph.
> Grafana shows the pipeline (throughput, lag, latency, risk) and the benchmarks measure it:
> a decision takes about 107 ms at p50 end to end, and one host sustains about 1,650 tx/s
> ([`docs/benchmarks/`](docs/benchmarks/streaming.md)).

## Quick start

Requirements: Docker with Compose v2, about 6 GB of free RAM, and
[uv](https://docs.astral.sh/uv/) for local development.

```bash
make env        # creates .env from .env.example - change the passwords
make up         # builds the image, starts everything, waits until healthy
make ps         # all services healthy; *-init jobs "Exited (0)"
curl -s localhost:8000/health/ready   # all five dependencies ok
```

Without make: `cp .env.example .env && docker compose up -d --wait`.

| URL | Service |
|---|---|
| http://localhost:8090 | **FraudX dashboard** (overview, live stream, investigations, graph) |
| http://localhost:8000/docs | FraudX API (OpenAPI) |
| http://localhost:8080 | Kafka UI |
| http://localhost:7474 | Neo4j browser |
| http://localhost:9090 | Prometheus |
| http://localhost:3000 | Grafana (dashboard *FraudX pipeline*) |

Every host port is set in `.env` (`API_PORT`, `MONGO_PORT`, `KAFKA_PORT`, …): change one there when it is taken, e.g. `MONGO_PORT=27018` next to a local MongoDB.

API endpoints (each one reads the store whose data model fits the query):

| Endpoint | Store | Query |
|---|---|---|
| `GET /transactions/{id}` | Cassandra | `transactions_by_id`, one partition |
| `GET /customers/{id}` | MongoDB | customer profile by `_id` |
| `GET /customers/{id}/transactions?day=&limit=` | Cassandra | one customer-day partition, newest first (default day: last activity, from MongoDB) |
| `GET /customers/{id}/network?limit=` | Neo4j | devices shared with other customers, money flows with counterparties |
| `GET /customers/{id}/risk` | Redis | latest risk score, set by the scorer in real time |
| `GET /fraud/alerts?day=&level=&limit=` | Cassandra | MEDIUM/HIGH transactions of a day (8 bucket partitions merged) |
| `POST /transactions/simulate` | Kafka | inject a transaction into the live pipeline; read it back with `GET /transactions/{id}` |
| `GET /fraud/investigations?status=` | MongoDB | analyst queue, newest first |
| `GET /fraud/{tx}` · `/fraud/{tx}/explanation` | MongoDB | case: risk breakdown, evidence, explanation with each reason's cited values |
| `PATCH /fraud/{tx}` | MongoDB + Neo4j | analyst decision; confirming fraud marks the payee account in the graph (known-fraud rule) |
| `POST /models/predict` | models | the live scorer's decision and explanation for one transaction, no side effects |
| `GET /stream/scored` | Kafka | live scored transactions (Server-Sent Events) |
| `GET /stats/overview` | Prometheus + MongoDB | dashboard KPIs (last 24 h) and analyst-confirmed precision |
| `GET /health`, `/health/ready`, `/metrics/` | all | liveness, dependency readiness, Prometheus |

## Development

```bash
make install            # uv sync: Python 3.12 venv with dev tools
make check              # ruff lint/format check + unit tests
make infra              # start only Kafka + databases (+ provisioning)
make api                # run the API on the host with auto-reload
make test-integration   # integration tests against the running stack
make data               # generate the synthetic dataset (data/generated)
make data-train && make features && make train   # train + register the fraud model
make replay             # publish it to transactions.raw (ARGS="--rate 500 --rebase-time")
make replay ARGS="--rate 20 --rebase-time --id-prefix LIVE1-"   # live demo: re-send as new transactions
make web                # dashboard dev server with hot reload (:5173)
make tail T=transactions.raw ARGS="--from-beginning -n 3"
make bench              # storage benchmarks -> docs/benchmarks/storage.md
make bench-stream       # end-to-end latency and throughput of the live pipeline
make help               # every command
```

## Repository layout

```text
fraudx/
├── src/fraudx/
│   ├── core/          # settings, logging, event contracts, topic names
│   ├── api/           # FastAPI app, routes, metrics
│   ├── storage/       # client factories, readiness probes (+ store writers)
│   ├── scoring/       # risk engine (+ ML, anomaly, graph features)
│   ├── ingestion/     # PaySim loader, synthetic generator, producer
│   ├── streaming/     # feature processor (Redis online features)
│   ├── explain/       # evidence retrieval + template/LLM explanations
│   └── alerts/        # alert dispatch
├── config/risk.yaml   # risk weights and thresholds
├── infra/             # kafka topics, cassandra/mongo/neo4j schemas, prometheus, grafana
├── tests/{unit,integration}/
├── data/{raw,processed,generated}/   # git-ignored datasets
├── models/            # git-ignored versioned model artifacts
├── docs/              # experiments, benchmarks, API notes
├── Dockerfile         # one image for all Python services
├── docker-compose.yml
└── Makefile
```

## Roadmap

| Phase | Deliverable | Status |
|---|---|---|
| 0 | Foundation: infra, schemas, contracts, risk engine, health API | done |
| 1 | Synthetic generator + PaySim loader → validated enriched events | done |
| 2 | Kafka producer and consumer framework (DLQ, metrics) | done |
| 3 | Feature processor (Redis online features) | done |
| 4 | Store writers: Cassandra, MongoDB, Neo4j | done |
| 5 | Supervised model (LightGBM, offline training, registry) | done |
| 6 | Anomaly detection (Isolation Forest, calibrated) | done |
| 7 | Graph features and graph model | done |
| 8 | Scorer service, calibrated risk engine, alerts | done |
| 9 | Investigations with evidence-grounded explanations (template / Claude) | done |
| 10 | API endpoints and React dashboard | done |
| 11 | Benchmarks and Grafana dashboards | done |
