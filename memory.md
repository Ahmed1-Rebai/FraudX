# FraudX Engineering Memory

Persistent project memory. **Read this first** when resuming work; update it
at the end of every work session (progress, issues, decisions, next tasks).

Last updated: 2026-10-07 (Phase 5, supervised model, complete)

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
make data              # synthetic dataset -> data/generated (ARGS="--customers 5000 --days 30")
make data-paysim       # data/raw/PS_log.csv -> data/processed/paysim (ARGS="--limit 100000")
make replay            # data/generated -> transactions.raw (ARGS="--rate 500 --rebase-time")
make tail T=transactions.dlq ARGS="--from-beginning -n 5"   # inspect a topic
make data-train         # training dataset (seed 1337, 5k customers, 30 days) -> data/training/synthetic
make features           # training features via online code in Redis db 1 (D=<dataset dir>)
make train              # build holdout features, train, report, register + activate in MongoDB
docker compose run --rm replay --rate 1000 --limit 5000     # replay from a container
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
- [x] **Phase 1, data:** deterministic synthetic generator (`ingestion/generator.py`)
      with labelled fraud scenarios and overlapping legit noise; PaySim loader
      (`ingestion/paysim.py`) with label-independent enrichment; atomic
      dataset writer (`ingestion/dataset.py`); CLI `python -m fraudx.ingestion`;
      `make data`. Default run: 63,716 events, 316 fraud (0.50 %), 0 invalid, 2.4 s.
      Tests: 51 unit (18 new), all passing
- [x] Stack recreated with the user's own passwords (the `.env` change needed fresh volumes)
- [x] **Phase 2, Kafka:** `messaging.producer.EventProducer`,
      `messaging.consumer.StreamWorker` (manual commits after flush, DLQ,
      backoff retries, graceful shutdown, metrics, lag, heartbeat),
      `ingestion.replay` (pacer, label stripping, time rebase), CLIs
      `python -m fraudx.ingestion replay` and `python -m fraudx.messaging tail`,
      compose job `replay` (profile `jobs`). Unthrottled replay of the full
      dataset: 63,716 events in 2.6 s (≈24k events/s), 0 failures, partitions
      evenly loaded. Tests: 64 unit, 10 integration (incl. a real-broker
      round trip: replay → worker → DLQ → resume from committed offsets)
- [x] **Phase 3, feature processor:** `TransactionFeatures` /
      `FeaturedTransaction` contracts (v1); `streaming.features`
      (`OnlineFeatureStore`: 3 Lua scripts in one pipelined round trip,
      event-time windows, result cache for idempotency, pure
      `build_features`); `streaming.processor` (validation → DLQ, label
      stripping); `python -m fraudx.streaming`; compose service
      `feature-processor` with heartbeat healthcheck; Prometheus DNS discovery.
      Backlog of 65,716 events processed at ≈2,050 events/s on one worker,
      0 dead-lettered. Tests: 75 unit, 16 integration
- [x] **Phase 4, store writers:** `StreamWorker` batch mode; `storage.graph_writer`
      (Neo4j, 3 × UNWIND per batch, ordered-guard counters),
      `storage.history_writer` (Cassandra, concurrent prepared upserts,
      UNSET for missing values, keyspace-qualified), `storage.profile_writer`
      (MongoDB bulk pipeline upserts, bounded recency lists); `storage.sink`
      batch handler; `python -m fraudx.storage graph|history|profile`; three
      compose services sharing an `x-worker` anchor; Prometheus discovers all
      workers with a `service` label. Money is now quantized to 2 dp in the
      contract. Backlog drained by all three writers in under 30 s; handler
      cost 0.35–0.39 ms/msg (≈2.5–2.9k msg/s per writer). Tests: 84 unit,
      21 integration
- [x] **Phase 5, supervised model:** `ml.vectorize` (23 features, shared with
      inference), `ml.dataset` (offline build through `OnlineFeatureStore.compute_many`
      on Redis db 1; refuses the live db; flushes afterwards), `ml.train`
      (LightGBM, time split, candidates, threshold, per-scenario metrics,
      ablations, versioned artifact), `ml.model.FraudModel` (inference, compatibility
      checks), `ml.registry` (MongoDB, one active version, rollback), `ml.report`,
      CLI `python -m fraudx.ml build-features|train`, make targets. The feature
      processor now runs in batch mode (`compute_many`). The image has
      `libgomp1` for LightGBM (467 MB). Model `fraud_lgbm:20261007140119-b6deb82e`
      trained and **active**. Tests: 92 unit, 24 integration

## Current progress

**Phase 5 (supervised model) complete.** Next: decide on generator v2
(see "Next tasks"), then Phase 6 (anomaly detection).

Model `fraud_lgbm:20261007140119-b6deb82e` (report:
`docs/experiments/2026-10-07-fraud_lgbm-20261007140119-b6deb82e.md`):
training set 327,819 events / 843 fraud; best iteration 56; threshold 0.14.

| set | PR-AUC | precision | recall | Brier |
|---|---|---|---|---|
| test (last 15 % by time) | 0.999 | 0.978 | 0.985 | 0.00012 |
| holdout (demo dataset, other seed) | 0.9985 | 0.972 | 0.984 | 0.00020 |

69 % of the gain comes from `cust_txn_count_5m` (card testing), followed by
`device_customer_count` (9 %) and `balance_drain_ratio` (6 %). Both
candidates reach validation PR-AUC 1.0, so the selection can't tell them
apart. **These numbers validate the pipeline, not real-world performance:**
the generator's fraud scenarios are separable by construction (see ADR-023).

Store contents after the backlog (65,716 events, of which 2,000 are a
rebased re-send of existing ids):
- Cassandra: 63,716 rows in `transactions_by_id` (the duplicates upserted)
- MongoDB: 2,000 customers, 2,000 accounts, 2,856 devices
- Neo4j: 2,000 Customer, 2,200 Account (200 merchants), 2,856 Device,
  3,973 IP, 21 Location; 56,681 TRANSFERRED_TO, 2,944 USES, 5,105
  CONNECTED_FROM. 7 devices are used by ≥3 customers (max 15): the fraud
  rings and their takeover victims, already visible in the graph.

Feature signal from Phase 3 (unchanged):

Feature signal on the default dataset (mean per scenario, from
`transactions.features` joined with ground truth):

| scenario | txns in 5 min | customers on device | balance drain | new device | amount z-score |
|---|---|---|---|---|---|
| account_takeover | 1.0 | 6.7 | 0.91 | 100 % | 62 |
| mule_cash_out | 1.0 | 8.2 | 0.95 | 21 % | 268 |
| card_testing | 5.9 | 3.2 | 0.03 | 9 % | 5 |
| large_purchase (legit) | 1.0 | 1.1 | 0.34 | 6 % | 35 |
| new_device (legit) | 1.0 | 1.0 | 0.05 | 95 % | 0.1 |
| normal | 1.0 | 1.07 | 0.04 | 4 % | 0.3 |

Default synthetic dataset signal check (fraud vs legit share of events):
foreign location 91 % vs 15 %, device shared by ≥3 customers 49 % vs 0.9 %,
night 26 % vs 4 %. The signals are strong but overlapping, so expect high
model scores on synthetic data. PaySim is the harder benchmark.

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
* `customers.country` is the first country seen, so a customer whose first
  transaction happened abroad gets the wrong home country (e.g. `C-000663`
  → AE). The stream has no KYC data. Fix by sourcing home country from
  customer master data, or deriving it as the modal country from Cassandra
  in a batch job.
* The 2,000 rebased test events (same ids, new times) appear twice in
  `transactions_by_customer` and advanced Neo4j edge counters. This is an
  artifact of re-sending the same dataset; `make clean && make up` resets it.
* A MongoDB validation failure inside a bulk write is treated as transient,
  so a schema bug would stall the profile writer (visible as lag) rather
  than dead-letter. Map write errors back to messages if this becomes a
  problem.
* Datasets share id spaces (`C-000001`, `TX-000000001`): replaying a
  *different* dataset into a running stack collides with existing ids (Redis
  result cache, Cassandra by-id rows, Mongo profiles). Use one dataset per
  stack (`make clean` between datasets), or add a dataset id prefix to the
  generator.
* The offline feature build shares the Redis instance (db 1, peak about 120 MB
  for 328k events, 512 MB limit, `volatile-lru`). A much larger build could
  evict live keys. Use a separate Redis for big builds.
* The synthetic data is easy to classify by construction. Report model
  metrics on PaySim as well, and break synthetic results down by `scenario`
  (ground_truth.csv).
* Card-testing bursts can run a few minutes past the configured end date.
  This is harmless.
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

### ADR-022: LightGBM, trained on features produced by the online code
- **Decision:** LightGBM (not XGBoost) for the supervised model. Training
  features are produced by replaying a dataset through
  `OnlineFeatureStore.compute_many` on an isolated Redis db, not by a
  separate pandas implementation. Model input is defined once in
  `fraudx.ml.vectorize`. Training uses a separate dataset (seed 1337); the
  demo dataset is a holdout.
- **Reason:** LightGBM trains fast on CPU, handles missing values (NaN) and
  categorical features natively, and has a small artifact (`model.txt`). That
  covers what we need; XGBoost would be equivalent. A second feature
  implementation for training is the classic source of training/serving
  skew. Training on the demo stream would make the live demo an in-sample
  evaluation.
- **Advantages:** Zero skew by construction (same code); reproducible builds
  from a dataset file; the version hash ties a model to its data; the
  batched build runs at about 5.7k events/s.
- **Trade-offs:** The training build needs Redis (not a pure-Python batch
  job); features must be built in event-time order; the image grew to
  467 MB (numpy, scikit-learn, LightGBM, libgomp).
- **Impact:** `fraudx.ml`; the scorer (Phase 8) loads the active registry
  version with `FraudModel`.

### ADR-023: Synthetic metrics are near-perfect, so the generator needs harder cases (proposed, not implemented)
- **Observation:** Test PR-AUC 0.999 and holdout 0.9985. Every v1 fraud
  scenario has a decisive signature: card testing → `cust_txn_count_5m`;
  takeover → shared ring device + balance drain; mule → drain + shared device.
- **Consequence:** The anomaly and graph detectors (Phases 6–7) can't show
  any measurable added value on this data, and the risk-engine weighting can't
  be justified empirically, which is the project's core claim.
- **Proposal:** Generator v2 adds realistic hard cases:
  - authorised push-payment fraud (victim's own device, home country,
    moderate amount to a mule; detectable mainly through the graph);
  - slow-drip mule activity under velocity thresholds;
  - legitimate bursts (e.g. several quick purchases);
  - legitimate large transfers to new payees.
  Then retrain and report per-detector contribution.
- **Status:** Awaiting the user's decision.

### ADR-021: Store writers are batched; they run on `transactions.features` until the scorer exists
- **Decision:** (a) Add a batch mode to `StreamWorker` (batch handler
  returns rejects; any exception rewinds the whole batch). (b) Run the
  Cassandra and MongoDB writers on `transactions.features` now, with risk
  columns unset, and switch them to `transactions.scored` in Phase 8. The
  graph writer stays on `.features` permanently.
- **Reason:** One store round trip per message caps sinks at a few hundred
  messages/s; batching gets about 2.5–2.9k/s per writer. Running the writers
  now fills all four stores early (the spec's Phase 3 deliverable), so
  ML/graph work and benchmarks have real data. Unset (not null) columns let
  the scored stream add risk later without conflicts.
- **Advantages:** Throughput; early end-to-end data; idempotent writes make
  batch retries safe.
- **Trade-offs:** A batch retry rewrites up to 500 records; one input-topic
  switch later (a config change plus a consumer-group offset decision).
- **Impact:** `fraudx.storage.*`, compose writer services.

### ADR-020: Online features in Lua with a per-transaction result cache
- **Decision:** Compute features inside Redis with Lua scripts (customer,
  device, IP), sent in one pipelined round trip. The customer script caches
  its raw result under `fx:cust:{C}:result:{tx}` (TTL = raw-topic retention);
  a redelivery returns the cached result. Windows use event time. Feature
  definitions are versioned (`FEATURE_VERSION`).
- **Reason:** Read-modify-write over several keys must be atomic, or two
  updates interleave and features drift. Counters like `HINCRBY` aren't
  idempotent, and at-least-once delivery will redeliver. One round trip per
  event keeps latency at about 0.5 ms. This replaces the planned `fx:seen:{tx}`
  flag: a flag alone would prevent double counting but couldn't return the
  original features.
- **Advantages:** Exactly-once *effect* on top of at-least-once delivery;
  identical output on replay; Redis Cluster compatible (one slot per script).
- **Trade-offs:** Business logic split between Lua (state updates) and Python
  (`build_features`, kept pure and unit-tested). Lua is covered by
  integration tests against real Redis. Device/IP windows aren't covered by
  the cache. They're idempotent, but a redelivery read after newer events can
  see a slightly higher device count.
- **Impact:** ARCHITECTURE §5.3. Changing a feature means bumping
  `FEATURE_VERSION` and retraining models.

### ADR-018: Transient errors block the partition; only poison messages go to the DLQ
- **Decision:** `StreamWorker` dead-letters a message only when the handler
  raises `PermanentError` (invalid or undecodable input). Any other error
  rewinds to the message and retries with capped exponential backoff,
  indefinitely, while continuing to poll.
- **Reason:** When a database is down, every message fails. Dead-lettering
  then would empty the stream into the DLQ and need a manual re-drive. Pausing
  is the correct response to an outage, and Kafka retains the backlog.
- **Advantages:** No data loss or DLQ flood during outages; recovery is
  automatic; consumer lag (metric) makes the stall visible.
- **Trade-offs:** A bug that raises a non-`PermanentError` on one specific
  message stalls that partition. This is mitigated by alerting on lag and
  retry counters, and handlers must classify errors carefully.
- **Impact:** Contract for every handler in Phases 3–9.

### ADR-019: No labels on the live stream
- **Decision:** `replay` strips `is_fraud_label` by default.
- **Reason:** Real transaction streams don't carry labels (fraud is
  confirmed days later). A label on the wire risks leaking into online
  features. Live "model accuracy" (spec dashboard) will come from joining
  `ground_truth.csv`, or from a delayed `transactions.labels` feedback topic
  if needed (simulating chargebacks).
- **Trade-offs:** Live accuracy needs that join or topic instead of reading
  the event.

### ADR-017: Dataset format: JSONL + ground-truth sidecar
- **Decision:** Datasets are directories with `enriched_transactions.jsonl`
  (one validated `TransactionEvent` per line), `ground_truth.csv`
  (`transaction_id, is_fraud, scenario`) and `summary.json`. This replaces
  the spec's single `enriched_transactions.json`.
- **Reason:** JSONL streams (no full-file parse), lines map 1:1 to Kafka
  messages, and partial writes are detectable. The fine-grained `scenario`
  is evaluation metadata and must not travel with events, where it would be
  a perfect leak. The event keeps only the binary `is_fraud_label`, which
  online scoring ignores.
- **Advantages:** Per-scenario recall analysis, reproducibility (summary
  stores the full generator config), atomic writes.
- **Trade-offs:** Three files instead of one.
- **Impact:** Producer (Phase 2) reads the JSONL; training (Phase 5) joins
  ground truth by `transaction_id`.

## Next tasks

1. **Decision:** generator v2 with hard cases (ADR-023), recommended
   *before* Phases 6–7 so the anomaly and graph detectors can show measurable
   value. Afterwards rerun `make data-train && make features && make train`.
2. **Phase 6 – Anomaly detection:** Isolation Forest trained on legitimate
   traffic only, from the same feature matrix; scores calibrated to [0, 1]
   (e.g. rank against a validation reference); registered like the
   supervised model.
3. Then graph features (7), anomaly (6), graph features (7), scorer + alerts (8),
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
* A summary built from a lazy generator must be computed **after** the
  stream is consumed. The PaySim CLI first reported `invalid_rows: 0`
  because the metadata dict was evaluated before the writer pulled any rows.
  `write_dataset` now takes a callable.
* PaySim's `nameOrig` is almost unique per row (each customer appears about
  once), so per-customer velocity/history features carry little signal on
  PaySim. This is one more reason the synthetic generator is the default
  for the streaming demo.
* Use stdlib `random.Random(seed)` with no set iteration for determinism:
  Python's string hashing is randomised per process, so iterating a `set` of
  ids would make the output differ between runs.
* **Always `set -o pipefail`** when piping test output (`make … | tail`).
  Without it, a failing test run reported success to a `&&` chain and a
  commit went ahead (Phase 1). The commit was sound, but the check was blind.
* Changing passwords in `.env` after the first start needs fresh volumes:
  MongoDB and Neo4j only read their credentials on first init.
* Rate pacers must reset their schedule from *now* after a stall. Otherwise
  they burst to "catch up" (found by unit test).
* Never iterate a dict you're popping from (`commit()` bug caught by unit test).
* librdkafka logs `Coordinator load in progress` right after topic or broker
  start. This is benign; the idempotent producer retries.
* A "nothing was redelivered" assertion is vacuous if the consumer may not
  have joined the group yet. Assert that a *new* message is delivered instead.
* Integration tests that share Redis must make **every** key run-unique,
  IPs included. A fixed test IP accumulated customers across runs and broke
  a distinct-count assertion.
* Redis Lua: return floats via `cjson.encode`; a bare Lua number is truncated
  to an integer reply. Use `cjson.null` (not `nil`) to keep a key in the table.
* Cassandra warns that `USE <keyspace>` with prepared statements is an
  anti-pattern. Connect without a keyspace and use `keyspace.table`.
* Pydantic `decimal_places` validates but doesn't normalise: `100` stayed
  `100` (not `100.00`) all the way into Cassandra. Quantize money in the
  contract.
* Pipelined Redis scripts execute in order, so batching many events in one
  pipeline gives identical per-customer results (verified by an integration
  test) at about 3× the throughput.
* LightGBM wheels need `libgomp1` at runtime; `python:3.12-slim` lacks it.
* Near-perfect metrics on synthetic data are a property of the generator,
  not a success of the model. Always break results down by scenario and ask
  what makes them separable.
* Prometheus must scrape `/metrics/` (trailing slash) because the metrics app
  is mounted as a sub-application.
