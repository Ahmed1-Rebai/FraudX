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

> **Status:** Phases 0–6 are complete: replayed transactions flow through Kafka, get online
> features in Redis, and land in Cassandra, MongoDB and Neo4j, which the API reads back.
> A versioned LightGBM model and an Isolation Forest anomaly detector are trained and
> registered, on the synthetic data and on PaySim and IBM AML HI-Small (reports in
> [`docs/data/`](docs/data/README.md) and [`docs/experiments/`](docs/experiments/)).
> Storage benchmarks are in [`docs/benchmarks/`](docs/benchmarks/storage.md).
> Next: graph features (Phase 7), then the scorer and alerts (Phase 8).

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
| http://localhost:8000/docs | FraudX API (OpenAPI) |
| http://localhost:8080 | Kafka UI |
| http://localhost:7474 | Neo4j browser |
| http://localhost:9090 | Prometheus |
| http://localhost:3000 | Grafana |

If port 8000 is busy, set `API_PORT` in `.env`.

API endpoints (each one reads the store whose data model fits the query):

| Endpoint | Store | Query |
|---|---|---|
| `GET /transactions/{id}` | Cassandra | `transactions_by_id`, one partition |
| `GET /customers/{id}` | MongoDB | customer profile by `_id` |
| `GET /customers/{id}/transactions?day=&limit=` | Cassandra | one customer-day partition, newest first (default day: last activity, from MongoDB) |
| `GET /customers/{id}/network?limit=` | Neo4j | devices shared with other customers, money flows with counterparties |
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
make tail T=transactions.raw ARGS="--from-beginning -n 3"
make bench              # storage benchmarks -> docs/benchmarks/storage.md
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
| 7 | Graph features | next |
| 8 | Scorer service, risk engine integration, alerts | |
| 9 | Evidence-grounded explanations (template / LLM) | |
| 10 | API endpoints and React dashboard | read endpoints done |
| 11 | Benchmarks and Grafana dashboards | storage benchmarks done |
