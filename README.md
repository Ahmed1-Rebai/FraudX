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

> **Status:** Phases 0–4 are complete: replayed transactions flow through Kafka, get online
> features in Redis, and land in Cassandra, MongoDB and Neo4j.
> Machine learning (Phase 5) is next. See [memory.md](memory.md#next-tasks).

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

## Development

```bash
make install            # uv sync: Python 3.12 venv with dev tools
make check              # ruff lint/format check + unit tests
make infra              # start only Kafka + databases (+ provisioning)
make api                # run the API on the host with auto-reload
make test-integration   # integration tests against the running stack
make data               # generate the synthetic dataset (data/generated)
make replay             # publish it to transactions.raw (ARGS="--rate 500 --rebase-time")
make tail T=transactions.raw ARGS="--from-beginning -n 3"
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
| 5–7 | Supervised model, anomaly detection, graph features | next |
| 8 | Scorer service, risk engine integration, alerts | |
| 9 | Evidence-grounded explanations (template / LLM) | |
| 10 | API endpoints and React dashboard | |
| 11 | Benchmarks and Grafana dashboards | |
