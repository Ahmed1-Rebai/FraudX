# FraudX Engineering Memory

Persistent project memory. **Read this first** when resuming work; update it
at the end of every work session (progress, issues, decisions, next tasks).

Last updated: 2026-10-07 (Phase 0, foundation)

---

## Project objective

Detect and explain suspicious financial transactions in near real time with
a polyglot NoSQL platform: Kafka streaming, online features (Redis), history
(Cassandra), profiles and investigations (MongoDB), relationship graph
(Neo4j), and a risk score that combines rules, supervised ML, anomaly
detection and graph features. Explanations are grounded in retrieved
evidence. MVP goal: one transaction flows from ingestion to a scored,
explained decision on the dashboard in near real time with `docker compose up`.

## Architecture (summary)

```text
producer → transactions.raw → feature-processor (Redis features) → transactions.features
  → scorer (rules + ML + anomaly + Neo4j graph features → risk engine) → transactions.scored
  → sinks: Cassandra history, MongoDB profiles, Neo4j graph (from .features), Redis latest risk
  → fraud.alerts → alert dispatcher + explainer (MongoDB investigations)
  → FastAPI → React dashboard;  Prometheus + Grafana
```

Full detail: [ARCHITECTURE.md](ARCHITECTURE.md). Original spec:
[FraudX_README.md](FraudX_README.md) (annotated where revised).

## Technology decisions (current stack)

| Area | Choice | Version |
|---|---|---|
| Language | Python | 3.12 (`.python-version`) |
| Packaging | uv + `uv_build`, single `src/fraudx` package, `uv.lock` | uv 0.9 |
| Streaming | Apache Kafka (KRaft) + Kafka UI (kafbat) | 4.1.0 / v1.3.0 |
| Stream processing | Python consumer-group workers + Redis state (ADR-001) | — |
| Documents | MongoDB | 8.0 |
| Wide-column | Cassandra | 5.0 |
| Key-value | Redis | 8.2 |
| Graph | Neo4j Community | 5.26 LTS |
| API | FastAPI + uvicorn | 0.14x |
| Kafka client | confluent-kafka (librdkafka) | 2.x |
| ML (Phase 5–6) | XGBoost or LightGBM, scikit-learn Isolation Forest | tbd |
| Observability | Prometheus + Grafana | 3.5 / 12.1 |
| Tests / lint | pytest, ruff | — |

## Database schemas

Source of truth is the provisioning code; ARCHITECTURE.md §5 explains it.

* Kafka topics: `infra/kafka/create-topics.sh` ↔ `src/fraudx/core/topics.py`
* Cassandra: `infra/cassandra/schema.cql` (`transactions_by_customer`,
  `transactions_by_id`, `risk_events_by_day`)
* MongoDB: `infra/mongodb/init.js` (`customers`, `accounts`, `devices`,
  `investigations`, `model_registry`, validators, indexes, app user)
* Neo4j: `infra/neo4j/schema.cypher` (uniqueness constraints, fraud-flag indexes)
* Redis: key design in ARCHITECTURE.md §5.3 (no provisioning needed)
* Event contract: `src/fraudx/core/schemas.py` (`TransactionEvent` v1, `RiskAssessment`)

## Conventions

* **Git**: the only author is `Ahmed1-Rebai`. No `Co-authored-by` trailers and
  no AI/tool attribution in commits, PRs or code. Conventional commit
  prefixes (`feat:`, `fix:`, `docs:`, `chore:`, `test:`, `refactor:`).
* **Code**: `src/fraudx/<area>/`; shared contracts in `core/`; client
  factories in `storage/clients.py`. Ruff (line length 100) is the formatter
  and linter. Type hints everywhere.
* **Config**: environment variables per service prefix (`KAFKA_`, `MONGO_`,
  `CASSANDRA_`, `REDIS_`, `NEO4J_`, `FRAUDX_`); secrets are `SecretStr` with
  no defaults. Risk parameters live in `config/risk.yaml`.
* **Events**: JSON, validated by Pydantic, `schema_version` field, additive
  changes only; aware UTC timestamps; money as `Decimal` (2 dp) in code,
  JSON number on the wire, `decimal` in Cassandra.
* **IDs**: natural keys (`C-…`, `A-…`, `DEV-…`, `TX-…`); Mongo `_id` = natural key.
* **Writes are idempotent** (at-least-once delivery everywhere).
* **Tests**: unit tests need no services (`make test`); integration tests are
  marked `integration` and need the stack (`make test-integration`).
* **Logging**: JSON to stdout via `fraudx.core.logging`; context goes in `extra=`.

## Commands

```bash
make env                # create .env from .env.example (then change passwords)
make install            # uv sync (incl. dev tools)
make check              # ruff + unit tests
make up                 # build + start the whole stack, wait for healthy
make infra              # only Kafka + 4 databases + provisioning
make api                # API on the host with reload (port 8000)
make test-integration   # integration tests against the running stack
make ps | logs S=api | topics | cqlsh | mongosh | redis-cli | cypher
make down               # stop (keep data)   |   make clean  # stop + delete volumes
```

Endpoints (local): API `:${API_PORT}` (`/health`, `/health/ready`, `/metrics/`,
`/docs`), Kafka UI `:8080`, Neo4j browser `:7474`, Prometheus `:9090`,
Grafana `:3000`.

## Completed tasks

- [x] Spec analysis and architectural review (ADR-001 … ADR-016)
- [x] Repository foundation: README, ARCHITECTURE, memory, .env.example,
      .gitignore, .dockerignore, Makefile, Dockerfile, docker-compose
- [x] Python package skeleton (`core`, `api`, `storage`, `scoring`, and
      placeholders for `ingestion`, `streaming`, `explain`, `alerts`)
- [x] Settings with per-service prefixes and validated risk config
- [x] Event contract `TransactionEvent` v1 and `RiskAssessment`
- [x] Risk engine (weights, thresholds, rule floors, missing-signal reweighting)
- [x] Infra provisioning: Kafka topics, Cassandra keyspace/tables, Mongo
      collections/validators/indexes/app user, Neo4j constraints
- [x] API: `/health`, `/health/ready` (5 dependencies + topics), `/metrics`
- [x] Prometheus scraping the API, Grafana with provisioned datasource
- [x] Tests: 33 unit, 9 integration, all passing; full stack verified healthy

## Current progress

**Phase 0 (foundation) complete.** Next is Phase 1 (data).

## Known issues

* Cassandra runs without authentication locally (the official image has no
  env switch; it needs a mounted `cassandra.yaml`). Mitigated by binding to
  127.0.0.1. Fix before any shared deployment.
* Kafka uses PLAINTEXT listeners (local only). Production needs SASL/TLS.
* Single-node everything: RF=1, no HA. Intended for local development.
* First `docker compose up` takes ~8 min (image pulls); later starts take ~1–2
  min, mostly Cassandra.
* Host port 8000 is taken on the main dev machine, so its `.env` sets
  `API_PORT=8001`.
* `kafka-ui` has no healthcheck defined yet (dev tool only, not a dependency
  of any service).

## Architectural decisions

Format: Decision / Reason / Advantages / Trade-offs / Impact.

### ADR-001: Python stream workers + Redis state instead of Apache Flink (MVP)
- **Decision:** Implement validation, enrichment and windowed features as
  Python consumer-group workers (confluent-kafka) that keep window state in
  Redis sorted sets keyed by event time. Flink is deferred, not rejected.
- **Reason:** The required features (per-customer counts/sums over 5 m/1 h/24 h,
  new device/IP/location, deviation from mean) are per-key sliding windows
  that Redis expresses exactly, and Redis is already the online feature store.
  PyFlink adds a JVM cluster (JobManager + TaskManager), connector JARs, a
  second runtime and slower debugging for no functional gain at MVP volumes.
  Flink's built-in windows emit on window close or slide, while fraud scoring
  needs "the last 5 minutes including this event" on every event.
- **Advantages:** One language and image; stateless, horizontally scalable
  workers; features are computed and served from the same store (no
  training/serving skew); replays are idempotent (`ZADD` by transaction id,
  dedupe key); far less memory and complexity in compose.
- **Trade-offs:** No Flink checkpoints/exactly-once; we rely on at-least-once
  plus idempotent writes. No watermark machinery; late events are accepted
  within the window TTL. Redis round-trips per event (pipelined) bound
  per-worker throughput, so scaling means more partitions and workers.
- **Impact:** `streaming/` is a Python service; spec §6 and Phase 4 revised.
  **Revisit** if we need complex event-time joins, CEP patterns, or
  sustained throughput beyond what N workers + Redis provide (≈ tens of
  thousands of events/s).

### ADR-002: Drop Debezium, Spark, Kubernetes and Terraform from the MVP
- **Decision:** These tools appear in the architecture image but are not part
  of the MVP.
- **Reason:** Debezium needs a source RDBMS and there is none (data enters via
  the producer). PaySim (~6.3 M rows, ~500 MB) fits in memory for
  pandas/polars training, so Spark adds a cluster without need. The spec
  itself says no Kubernetes for the first milestone.
- **Advantages:** Smaller surface, faster startup, every component justified.
- **Trade-offs:** Large-scale batch backfills would need Spark later.
- **Impact:** Not in compose or the structure; listed as future extensions.

### ADR-003: Revised Kafka topic design
- **Decision:** Topics `transactions.raw`, `transactions.features`,
  `transactions.scored`, `transactions.dlq`, `fraud.alerts`. Merge the spec's
  `transactions.enriched` into `.features`; rename `fraud.predictions` to
  `transactions.scored`; add a DLQ. Key by `customer_id`; keyed topics
  co-partitioned (6). Auto-create disabled.
- **Reason:** Validation, enrichment and feature computation happen in one
  step, so a separate `.enriched` topic would have no distinct consumer.
  Every transaction gets scored (not only fraud), so `transactions.scored`
  names it accurately. Invalid events must not be dropped silently or block a
  partition.
- **Advantages:** Fewer hops, clear contracts per stage, poison messages
  isolated, per-customer ordering for windows.
- **Trade-offs:** Hot customers map to one partition (acceptable; can salt
  keys later). Partition count is fixed early.
- **Impact:** `core/topics.py`, `create-topics.sh`, spec §5 annotated.

### ADR-004: Neo4j stores entities and aggregated edges, not transaction nodes
- **Decision:** No `:Transaction` nodes. Money flow is
  `(:Account)-[:TRANSFERRED_TO {count, total_amount, first_seen, last_seen}]->(:Account)`;
  usage edges carry counters and first/last seen.
- **Reason:** One node per event makes the graph grow with event volume and
  slows multi-hop traversals; graph features (shared devices, neighbours,
  distance to fraud) only need entity connectivity. History belongs in Cassandra.
- **Advantages:** Bounded graph size, fast traversals, idempotent `MERGE`
  writes, clear separation from history.
- **Trade-offs:** Can't traverse individual transactions in the graph; the
  investigation view joins Cassandra rows with the entity subgraph. Edge
  counters aren't replay-idempotent unless guarded (dedupe key in Redis).
- **Impact:** `schema.cypher`; spec §10 annotated.

### ADR-005: Cassandra data model refinements
- **Decision:** Three query tables (`transactions_by_customer`,
  `transactions_by_id`, `risk_events_by_day` with 8 buckets); `decimal` for
  money; `event_time DESC` clustering; TWCS compaction.
- **Reason:** Spec needs `GET /transactions/{id}` and an alert feed, which the
  single proposed table can't serve without `ALLOW FILTERING`. `double` loses
  cents. Newest-first is the dominant read.
- **Advantages:** Every API query is a single-partition read.
- **Trade-offs:** Denormalised writes (3 tables per scored event).
- **Impact:** `schema.cql`, ARCHITECTURE §5.1.

### ADR-006: MongoDB conventions
- **Decision:** Natural key as `_id`, `$jsonSchema` validators, capped arrays,
  least-privilege app user, `model_registry` collection for model versions.
- **Reason:** Avoid an extra unique index per collection; protect
  data quality; prevent unbounded document growth; spec requires versioned models.
- **Advantages:** Simpler queries, enforced shape, auditable model lineage.
- **Trade-offs:** Validators must evolve with the code (moderate level eases this).
- **Impact:** `init.js`.

### ADR-007: Redis key design
- **Decision:** `fx:` prefix, per-customer hash tags `{C}`, TTL on every key,
  ZSET sliding windows by event time, `fx:seen:{tx}` dedupe guard.
- **Reason:** Cluster-ready multi-key operations; bounded memory; exact,
  replay-safe windows.
- **Trade-offs:** Hash tags pin a customer to one slot (fine; customers are many).
- **Impact:** ARCHITECTURE §5.3; implemented in Phase 3.

### ADR-008: Risk engine semantics
- **Decision:** Weighted average over *available* signals (weights
  re-normalised), rule hits impose a minimum score, round-half-up, config
  validated on load (weights sum to 1, thresholds ordered).
- **Reason:** The spec formula treats a missing detector as 0, which hides
  outages as "low risk". The spec's design principles require rules alongside
  ML, but the formula had no place for them.
- **Advantages:** Graceful degradation that is still reported
  (`missing_signals`); deterministic overrides for known-bad patterns.
- **Trade-offs:** A score computed from fewer signals is less reliable; the
  API surfaces `missing_signals` so analysts can see it.
- **Impact:** `scoring/risk.py`, `config/risk.yaml`.

### ADR-009: Explanations are provider-agnostic and default to offline templates
- **Decision:** `explain` builds a structured evidence object, then renders
  via a `template` generator (default, deterministic) or an opt-in LLM
  provider. Only alerts or analyst requests trigger explanations. Every
  reason must cite evidence keys; uncited reasons are dropped. IDs are
  pseudonymised and raw IPs/balances withheld from external LLMs.
- **Reason:** The spec requires demo without external dependencies, no
  fabricated evidence, and no unnecessary sensitive data to the LLM. The
  architecture image names OpenAI, the README names no provider.
- **Advantages:** Works offline, testable, swappable provider, auditable.
- **Trade-offs:** Template explanations read less naturally.
- **Impact:** `LLM_PROVIDER` env; Phase 9.

### ADR-010: JSON + versioned Pydantic contracts (no Schema Registry yet)
- **Decision:** JSON payloads validated by `fraudx.core.schemas` with a
  `schema_version` field; additive changes only.
- **Reason:** All producers/consumers are Python in one repo sharing the
  models; a Schema Registry + Avro adds a service and codegen for no current benefit.
- **Trade-offs:** Larger messages than Avro; compatibility enforced by review
  and tests rather than the broker side.
- **Impact:** Revisit when a non-Python consumer appears (Avro/Protobuf + registry).

### ADR-011: Single Python package and image (src layout, uv)
- **Decision:** Replace the spec's top-level `ingestion/`, `streaming/`,
  `ml/`, `risk_engine/`, `llm/`, `api/` folders with subpackages of
  `src/fraudx/`, built into one image; services differ by command.
- **Reason:** Shared contracts/config/clients would otherwise be duplicated
  or hacked in via `sys.path`. One lock file means reproducible builds.
- **Advantages:** One dependency graph, one image to build, imports that work
  in tests, easy refactoring.
- **Trade-offs:** The image includes code for all services; heavy ML deps
  can move to an optional extra/separate image if size becomes a problem.
- **Impact:** Project structure in README and spec §17.

### ADR-012: Kafka in KRaft mode; kafbat Kafka UI
- **Decision:** `apache/kafka` 4.1 single KRaft node with internal/external
  listeners; `kafbat/kafka-ui` instead of `provectuslabs/kafka-ui`.
- **Reason:** ZooKeeper is removed in Kafka 4; provectus UI is unmaintained
  and kafbat is its maintained fork.
- **Impact:** compose.

### ADR-013: Built-in synthetic data generator is the default source
- **Decision:** Ship a generator producing PaySim-shaped transactions with
  devices/IPs/locations and injected fraud patterns (rings sharing devices,
  velocity bursts, unusual geos). PaySim from Kaggle is an optional input.
- **Reason:** Kaggle requires an account/API token; the spec requires demos
  without external dependencies. PaySim also lacks device/IP data entirely.
- **Trade-offs:** Graph/device signals are synthetic by construction; report
  ML metrics on PaySim columns separately from enrichment-driven signals to
  avoid circular evaluation.
- **Impact:** Phase 1 deliverables.

### ADR-014: Secure-by-default local stack
- **Decision:** Ports bound to 127.0.0.1; auth on Mongo/Redis/Neo4j;
  least-privilege Mongo app user; non-root app container; required secrets
  fail compose fast (`${VAR:?}`); passwords passed via env, not command args.
- **Trade-offs:** Slightly more setup (`make env`). Cassandra auth deferred
  (see Known issues).

### ADR-015: API-first; dashboard after the pipeline works
- **Decision:** React dashboard is Phase 10; FastAPI (with SSE for the live
  stream) is built first, and Grafana covers system metrics meanwhile.
- **Reason:** The spec prioritises a working end-to-end MVP.

### ADR-016: ML evaluation protocol for PaySim
- **Decision:** Time-based split on `step`; PR-AUC primary metric plus
  precision/recall/F1/ROC-AUC; report with and without balance features.
- **Reason:** Fraud is ~0.13% of PaySim; accuracy is meaningless. PaySim's
  balance columns are known to leak the label (fraud empties the origin
  account, and destination balances are inconsistent), so near-perfect scores
  must be checked against a model without them. Fraud only occurs in
  TRANSFER and CASH_OUT.
- **Impact:** Phase 5 training scripts and experiment notes in `docs/`.

## Next tasks

1. **Phase 1 – Data:** synthetic generator (`fraudx.ingestion.generator`) with
   fraud-pattern injection; PaySim loader (`step` → event time, `nameOrig` →
   customer/account mapping); validate against `TransactionEvent`; write
   `data/generated/enriched_transactions.jsonl`; unit tests.
2. **Phase 2 – Kafka:** producer (rate-controlled replay, idempotent
   producer, `acks=all`), consumer base class (manual commit, DLQ, metrics,
   graceful shutdown); integration test raw → consumer.
3. **Phase 3 – Feature processor:** Redis sliding windows + profile stats;
   publish `transactions.features`; `FeatureVector` contract.
4. **Phase 4 – Sinks:** Cassandra, MongoDB, Neo4j writers.
5. Then ML (5), anomaly (6), graph features (7), scorer + alerts (8),
   explainer (9), API endpoints + dashboard (10), benchmarks + Grafana
   dashboards (11).

## Lessons learned

* Since MongoDB 4.0.5, `listDatabases` doesn't fail for unprivileged users; it
  returns only the databases they can access. To test least privilege, use a
  write to another database.
* cassandra-driver ≥ 3.29 ships the libev reactor in its wheels, so it works on
  Python 3.12 (no asyncore). Pass load balancing through an
  `ExecutionProfile`; the legacy `Cluster(load_balancing_policy=…)` is deprecated.
* Starlette's TestClient now warns on `httpx`; the dev dependency is
  `httpx2` (import name `httpx2`).
* Re-enabling propagation on `uvicorn.access` undoes `--no-access-log`; only
  re-route `uvicorn` and `uvicorn.error`.
* Prometheus must scrape `/metrics/` (trailing slash) because the metrics app
  is mounted as a sub-application.
