# FraudX Engineering Memory

Persistent project memory. **Read this first** when resuming work; update it
at the end of every work session (progress, issues, decisions, next tasks).

Last updated: 2026-10-16 (review fixes; PaySim + IBM AML benchmarks trained; synthetic model retrained on 24 features and active; see "Review 2026-10-16" in Known issues)

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
make download-datasets # Kaggle: PaySim + IBM AML HI-Small -> data/raw (token in ~/.kaggle/access_token)
make paysim            # PaySim: profile -> clean -> canonical -> features -> train -> report
make ibm-aml           # IBM AML: same five stages -> data/processed/ibm_aml
make replay            # data/generated -> transactions.raw (ARGS="--rate 500 --rebase-time")
make tail T=transactions.dlq ARGS="--from-beginning -n 5"   # inspect a topic
make data-train         # training dataset (seed 1337, 5k customers, 30 days) -> data/training/synthetic
make features           # training features via online code on redis-offline (D=<dataset dir>)
make train              # build holdout features, train, report, register + activate in MongoDB
make offline-redis      # start the disposable Redis (:6380) used by feature builds
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
- [x] **Phase 5, supervised model:** `ml.vectorize` (features shared with
      inference; now 24 after adding `payment_method`), `ml.dataset` (offline
      build through `OnlineFeatureStore.compute_many`; originally on Redis db 1,
      now on the disposable `redis-offline` service, ADR-026), `ml.train`
      (LightGBM, time split, candidates, threshold, per-scenario metrics,
      ablations, versioned artifact), `ml.model.FraudModel` (inference, compatibility
      checks), `ml.registry` (MongoDB, one active version, rollback), `ml.report`,
      CLI `python -m fraudx.ml build-features|train`, make targets. The feature
      processor now runs in batch mode (`compute_many`). The image has
      `libgomp1` for LightGBM (467 MB). Model `fraud_lgbm:20261007140119-b6deb82e`
      trained and **active**. Tests: 92 unit, 24 integration
- [x] **Generator v2 (ADR-023):** hard fraud cases (`app_scam`,
      `account_takeover_local`, `mule_slow_drip`) and legitimate look-alikes
      (`legit_burst`, `new_payee`); v1 behaviour available by zero weights.
      Both datasets regenerated; model retrained:
      `fraud_lgbm:20261007200714-47e7e074` **active** (v1 model still
      registered for rollback). Tests: 97 unit, 24 integration
- [x] **Public datasets (ADR-024/025/026):** `fraudx.datasets` pipeline
      (`base`, `paysim`, `ibm_aml`, `info`, `report`, CLI) with explicit,
      re-runnable stages profile → clean → canonicalize → report; the
      hash-based PaySim enrichment is gone (`ingestion/paysim.py` deleted,
      nothing invented). PaySim + IBM AML HI-Small downloaded with the user's
      Kaggle token; **profile, clean and canonicalize run for both**; reports
      generated in `docs/data/`. Additive `TransactionEvent` v1 fields
      (`payment_method`, `original_amount`, `original_currency`) and a
      `payment_method` model feature + `time`/`channel` ablation groups.
      Offline builds moved to a disposable `redis-offline` service
      (`OnlineFeatureStore(cache_results=False)`); `ml build-features` refuses
      to run without it. Polars added. Tests: 101 unit, 24 integration
- [x] **Phase 11, observability + streaming benchmark:** Grafana dashboard
      *FraudX pipeline* provisioned (20 panels: health, latency, risk,
      API/Kafka); `python -m fraudx.bench_stream` / `make bench-stream`:
      steady 200 tx/s → p50 107 ms, p95 162 ms, p99 487 ms end to end; burst
      of 20k → 1,659 tx/s sustained (`docs/benchmarks/streaming.md`). Found and
      fixed: batch workers waited up to 1 s per batch (p50 was 1.3 s)
- [x] **Phase 10, dashboard (ADR-032):** `frontend/` React + TypeScript
      (Vite), nginx image `dashboard` (:8090, `/api` proxy, SSE-safe); pages
      overview (KPIs via `GET /stats/overview`), live stream (`GET
      /stream/scored`, SSE), investigations (case + cited explanation +
      decision), graph (radial SVG). `replay --id-prefix` re-sends a dataset
      as new transactions for live demos. `make up` reloads Prometheus (SIGHUP).
      Tests: 135 unit, 33 integration; screenshots checked in both themes
- [x] **Phase 9, investigations + explanations (ADR-009):** alert handler
      (`python -m fraudx.alerts`, compose `alert-handler`) → MongoDB
      investigation with evidence + grounded explanation (template default,
      Claude opt-in) + optional webhook; API `/fraud/investigations`,
      `/fraud/{tx}`, `/fraud/{tx}/explanation`, `PATCH /fraud/{tx}`.
      295 investigations on the demo data. Tests: 133 unit, 33 integration
- [x] **Phase 8, scorer (ADR-030/031):** `scoring.scorer` (`python -m
      fraudx.scoring`, compose `scorer`): calibrated signals + rules → risk
      engine → `transactions.scored` / `fraud.alerts` / Redis latest risk;
      history and profile writers moved to `transactions.scored` (risk
      columns, `risk_events_by_day`, `risk_profile`); API `/fraud/alerts`,
      `/customers/{id}/risk`, `POST /transactions/simulate`. Live replay of the
      demo data: alert precision 0.790, recall 0.879 (offline 0.792 / 0.876).
      Tests: 122 unit, 32 integration
- [x] **Phase 7, graph features (ADR-029):** `storage.graph_features` (6
      point-in-time features, one Neo4j query per batch), offline build by
      replay into `neo4j-offline` (`build-graph-features`), graph model
      `train --graph` (`graph_lgbm`). Active
      `graph_lgbm:20261016230327-f3e4f4f4`: synthetic test PR-AUC 0.141
      alone, but **app_scam recall 100 %** (supervised: 0 %)
- [x] **Phase 6, anomaly detector (ADR-028):** `ml.anomaly`: Isolation
      Forest on legitimate training rows, numeric features, median fill,
      calibrated to the percentile among legitimate validation traffic;
      `python -m fraudx.ml train-anomaly` (in `make train/paysim/ibm-aml`).
      Active `anomaly_iforest:20261016225000-97be2705`. Test PR-AUC: synthetic
      0.250 (holdout 0.533), PaySim 0.078, IBM AML 0.002 (= base rate).
      Tests: 109 unit, 29 integration
- [x] **Read side + storage benchmarks (ADR-027):** `storage.readers`
      (Cassandra single-partition reads, MongoDB `_id` lookup, Neo4j patterns
      anchored on one customer); API `GET /transactions/{id}`,
      `/customers/{id}`, `/customers/{id}/transactions`, `/customers/{id}/network`
      (store clients connected on first use); `python -m fraudx.bench` /
      `make bench` → `docs/benchmarks/storage.md`
- [x] **Review fixes (2026-10-16):** graph counter guard on
      `(event_time, transaction_id)`; `ml register` command;
      prevalence-matched PR-AUC/precision in every report. **Benchmarks
      trained:** PaySim (test 0.982, matched 0.957), IBM AML (0.147, the
      graph-detector baseline); synthetic demo model retrained on 24 features
      and active

## Current progress

**Phase 5 + generator v2 complete; public-dataset pipeline in place.** The
PaySim and IBM AML datasets are downloaded and **profiled, cleaned and
canonicalized** (`data/processed/{paysim,ibm_aml}/`, all git-ignored) with
reports in `docs/data/{paysim,ibm_aml}.md`.

State as of 2026-10-16: **PaySim trained** (`fraud_lgbm:20261016223525-1d8b4943`,
registered, inactive; deterministic rerun of 20261014152329 with the same
metrics): test PR-AUC 0.982, **0.957 at the dataset's fraud rate**, 0.416
without balance features (`docs/data/paysim.md`). **IBM AML trained**
(`fraud_lgbm:20261016223640-22c237be`, registered, inactive): test PR-AUC
**0.147** (0.124 at the dataset's rate), recall 0.26 at precision 0.21;
pattern recall 35–52 %, `laundering_unattributed` 3 %. Without
`payment_method` PR-AUC falls to 0.037 (ACH carries most laundering), so
per-transaction features barely see laundering: this is the baseline the
graph detector (Phase 7) must beat (`docs/data/ibm_aml.md`). The weighted
candidate early-stopped at iteration 1 on IBM AML (unweighted won).

Public-dataset canonical sizes (after cleaning):
- PaySim: 6,362,335 events, 7,928 fraud (0.125 %), 30 days.
- IBM AML: 4,481,694 events, 4,514 laundering (0.101 %), 10 days,
  122,767 customers (corporate hubs; ~35 min per feature build).

**Datasets (v2):** demo `data/generated` 65,292 events / 265 fraud (0.41 %);
training `data/training/synthetic` 336,653 / 684 (0.20 %).
The live stack holds the **v2 demo dataset** (replayed 2026-10-16 through the
full pipeline). `make clean` wipes it and the MongoDB model registry
(then `make up && make replay` and `make train`).

Active model `fraud_lgbm:20261016223227-97be2705` (report in
`docs/experiments/`), trained on v2 with the 24 current features (the
earlier `20261007200714-47e7e074` has 23 features and no longer loads); its
metrics equal the earlier run:

| set | PR-AUC | precision | recall |
|---|---|---|---|
| test | 0.775 | 0.793 | 0.697 |
| holdout (demo v2) | 0.905 | 0.865 | 0.868 |
| *v1 model on v1 data, for comparison* | *0.999* | *0.978* | *0.985* |

Test recall by scenario: app_scam **0 %** (needs the graph detector),
mule_slow_drip 58 %, card_testing 87.5 %, takeovers and mule cash-out 100 %
(only 3–6 cases each). FPR: new_payee 1.4 %, large_purchase 1.6 %,
legit_burst 0.07 %. Without balance features PR-AUC falls 0.775 → 0.617.
Candidates now differ (validation 0.72 unweighted vs 0.61 weighted). The
holdout scores higher than test because its scenario mix holds more
card testing.

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
* Feature-store cost grows with a customer's 24 h activity (the Lua window
  sum iterates the window): about 450 µs per event on IBM AML corporates
  versus 60 µs on synthetic data. Offline builds of IBM AML take about 35 min.
  Bounded alternative: hierarchical time buckets (see ARCHITECTURE §5.3).
* The synthetic data is easy to classify by construction. Report model
  metrics on PaySim as well, and break synthetic results down by `scenario`
  (ground_truth.csv).
* Card-testing bursts can run a few minutes past the configured end date.
  This is harmless.
* `kafka-ui` has no healthcheck defined yet (dev tool only, not a dependency
  of any service).

**Review 2026-10-16 (open findings, measured):**

* ~~Graph edge counters drop same-timestamp events.~~ **Fixed 2026-10-16.**
  The `ts > last_seen` guard couldn't tell a redelivery from a second transfer
  in the same millisecond. On IBM AML (minute resolution) 97,327 rows (2.2 %,
  224 of them laundering) share payer, payee and minute. The guard is now
  `(ts, transaction_id)` with `last_tx` on the edge, and batch rows are sorted
  by the same key. Regression test: `test_graph_counts_distinct_transactions_with_the_same_timestamp`.
* ~~PaySim test PR-AUC is inflated by prevalence.~~ **Reported 2026-10-16.**
  A row-count time split puts days 15–29 in test (fraud rate 0.39 %) and days
  13–15 in validation (0.059 %). Reports now add PR-AUC/precision at the
  dataset's rate (0.125 %): PaySim test 0.982 → **0.957**, validation 0.940 →
  0.968. (Test without the flagged inflated days: 0.924.)
* ~~`register_paysim_model.py` wrote a wrong registry doc~~ **Fixed
  2026-10-16:** `python -m fraudx.ml register <artifact>` records a model from
  its `metadata.json`; the 2026-10-14 entry was re-registered with it (correct
  UTC time, threshold, 24 features) and the script deleted.
* The risk engine's alert threshold (70) ignores the model's own threshold
  (PaySim 0.49, from validation). With ML alone, alerts need p ≥ 0.70. Map or
  calibrate signals in Phase 8.
* ~~The NoSQL stores are write-only.~~ **Fixed 2026-10-16:** read endpoints
  (`storage.readers` → API) and `make bench` (see ADR-027).
* Cassandra TWCS tables have no `default_time_to_live`, so TWCS can't drop
  whole SSTables. Either set a TTL (retention policy) or justify TWCS without one.

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
- **Impact:** `EXPLAIN_PROVIDER` env; Phase 9.
- **Implemented 2026-10-17:** `fraudx.explain` (evidence, template with 12
  reason rules, `ClaudeExplainer`: `claude-opus-5-5`, schema-constrained JSON,
  effort low, `fallbacks: "default"`, template fallback on any failure) and
  `fraudx.alerts` (one investigation per alert via `$setOnInsert`, optional
  webhook). Evidence keys contain dots; MongoDB 8 stores them inside the
  `evidence` sub-document (never queried by field).

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
- **Update 2026-10-16:** PR-AUC and precision are also reported at the
  dataset's overall fraud rate (`at_reference_prevalence`, positives
  reweighted). A time split gives each set a different base rate (PaySim
  test 0.39 % vs validation 0.059 %), and both metrics move with it.

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

### ADR-032: Dashboard: React + TypeScript behind nginx, SSE for the live stream, analyst-confirmed precision
- **Decision:** A Vite React TypeScript app with React as the only runtime
  dependency (charts and graph in hand-written SVG), built into an nginx image
  that proxies `/api`. The live stream is Server-Sent Events from
  `transactions.scored` (one throw-away consumer group per viewer, from the
  topic end). KPIs come from Prometheus (the counters already exist) and
  MongoDB; "model accuracy" is the analyst-confirmed precision of decided
  investigations.
- **Reason:** The spec asks for React + TypeScript. SSE is one-way, works
  through a plain HTTP proxy and reconnects by itself, which is all a live
  feed needs (no WebSocket). Live traffic has no labels (ADR-019), so the
  only honest live accuracy is the analysts' verdicts.
- **Trade-offs:** KPIs depend on Prometheus (the endpoint reports
  `metrics_available: false` and keeps the MongoDB figures if it is down).
  Each SSE viewer is one Kafka consumer. Rebased replays faster than real
  time produce future event times (the intervals are preserved).
- **Impact:** `frontend/`, `api/routes/{stats,stream}.py`,
  `api/prometheus.py`, compose `dashboard`, ARCHITECTURE §13.

### ADR-031: Scorer runtime: graceful degradation, hot model reload, writers on the scored stream
- **Decision:** One scorer service (batch 500) computes graph features
  (Neo4j), three calibrated signals and rules per batch. A Neo4j error drops
  only the graph signal for that batch; no model at all retries (lag). Active
  model ids are checked every 60 s and reloaded on change. History and
  profile writers consume `transactions.scored` (planned in ADR-021); the
  graph writer stays on `transactions.features`. `rule_hits` became
  `frozen<list<text>>`.
- **Reason:** ADR-008's missing-signal semantics need a real degraded mode,
  not a stall. One Cassandra write per transaction including its risk (no
  second update pass). A non-frozen collection INSERT writes a tombstone to
  clear the old value: one tombstone per scored row.
- **Trade-offs:** History now depends on the scorer being up (Kafka keeps
  the backlog, so nothing is lost). Changing `rule_hits` to frozen needed a
  one-time keyspace drop and re-provision on the dev stack (2026-10-16);
  Cassandra was refilled by replaying `transactions.scored`.
- **Impact:** `scoring/scorer.py`, `scoring/__main__.py`, compose `scorer`
  (mounts `./models`, `./config`), `storage/{history,profile}_writer.py`,
  `storage/sink.py`, `schema.cql`, ARCHITECTURE §6.4.

### ADR-030: Risk engine: calibrated signals, weights and thresholds tuned on validation
- **Decision:** Keep the spec's weighted average (ADR-008), but (a) every
  signal is a calibrated fraud probability (isotonic regression on
  validation, stored in each model's metadata), (b) weights are chosen by a
  grid search (0.1 steps) on validation PR-AUC, ties to 0.4/0.3/0.3, (c)
  HIGH/alert = best-F1 score, MEDIUM = best-F2 score on validation, (d) two
  rules with ≥ 0.85 precision set floors (`takeover_pattern` → HIGH,
  `burst_on_shared_device` → MEDIUM). `make tune-risk` writes
  `config/risk.yaml` and a report.
- **Reason:** Measured on the synthetic test split: the spec's 0.4/0.3/0.3
  on raw scores gives PR-AUC 0.717, *below* ML alone (0.747), because the
  anomaly percentile is ≈ 0.5 on normal traffic and drowns the others.
  Calibrated: 0.838; tuned: **0.851**; a learned stacking model 0.876 on test
  but equal on the holdout (0.931 vs 0.932), so the interpretable,
  configurable formula is kept.
- **Result:** weights ml 0.5 / anomaly 0.1 / graph 0.4; MEDIUM ≥ 8, HIGH ≥ 23.
  Alerts: test precision 0.74 / recall 0.76; holdout 0.79 / 0.88 (alert rate
  0.2–0.45 %). Without graph: test 0.768; without anomaly 0.847 (it adds little).
- **Trade-offs:** Weights and thresholds are tied to the model versions:
  re-tune after retraining. Calibration uses the validation split that also
  chose early stopping and the model's threshold. Thresholds look low (23)
  because the score is a probability-weighted average: 23 ≈ 23 % fraud odds.
- **Impact:** `ml/calibration.py`, `scoring/{detectors,rules,tuning}.py`,
  `config/risk.yaml`, CLI `tune-risk`, ARCHITECTURE §6.

### ADR-029: Graph features: point-in-time Neo4j query, offline replay, separate graph model
- **Decision:** Six graph features per transaction (`payee_payer_count`,
  `payee_payee_count`, `payer_payee_count`, `is_new_payee`,
  `customer_ring_size`, `payee_owner_ring_size`), defined in money-flow terms
  and **as of the event time** (only edges with `first_seen` before it). One
  `UNWIND` query per batch, live and offline. Training data comes from
  replaying the dataset into an empty `neo4j-offline` with the production
  `GraphWriter`, reading each one-minute group's features before writing it.
  A separate LightGBM on these features is the `graph` signal.
- **Reason:** The supervised model misses fraud whose only tell is the
  payee (`app_scam`: 0 % recall) and laundering structures. Computing graph
  features with the live query on a replayed graph keeps training and serving
  identical (the ADR-022 principle), and the `first_seen < event time` filter
  makes them independent of graph-writer lag. A separate model keeps the
  signal explainable ("graph risk") and weightable.
- **Advantages:** No skew, no leakage of the transaction into its own
  features, anchored (indexed) patterns; the same query serves the scorer.
- **Trade-offs:** Offline build ≈ 700 events/s (synthetic training set 8 min,
  IBM AML ~1.5–2 h); live features can be a few seconds stale; PaySim is not
  built (its graph is nearly empty). Within a one-minute group a transaction
  doesn't see the others.
- **Result (synthetic):** graph model alone test PR-AUC 0.141 (holdout 0.218)
  but app_scam and both takeovers at 100 % recall, new_payee FPR 1.4 %;
  without the device-ring features 0.011.
- **Result (IBM AML, 2026-10-17):** graph model alone test PR-AUC 0.053
  (one-hop counts; no devices in IBM, so half the features are 0), but in
  the combined engine (`tune-risk --model ...`, report
  `docs/experiments/2026-10-17-risk-engine-ibm_aml.md`) it lifts PR-AUC to
  **0.197** from 0.138 without it (supervised alone 0.147 raw / 0.132
  calibrated). Stacking reaches 0.253 there: on laundering data a learned
  combiner is worth revisiting. The 3.7 h offline build is the cost.
- **Impact:** `storage/graph_features.py`, `ml/graph_dataset.py`, CLI
  `build-graph-features`, `train --graph`, compose `neo4j-offline`,
  `make offline-neo4j/features/train/ibm-aml`, ARCHITECTURE §6.3.

### ADR-028: Anomaly detector: Isolation Forest on legitimate traffic, percentile-calibrated
- **Decision:** `fraudx.ml.anomaly` fits an Isolation Forest (200 trees, 256
  samples) on the legitimate rows of the training split, numeric features
  only, missing values filled with training medians. The raw score is mapped
  to its percentile among legitimate validation traffic; threshold 0.99.
- **Reason:** The risk engine needs an `anomaly` signal in [0, 1] whose
  meaning is stable across datasets; a percentile among normal traffic is
  that, without labels. Training on legitimate rows keeps known fraud out of
  "normal". Same matrix and split as the supervised model, so no new
  feature code.
- **Advantages:** Label-free, fast (seconds on 6 M rows), interpretable score.
- **Trade-offs:** Unusual is not fraudulent: on synthetic data it catches
  takeovers/cash-outs (100 %) and card testing (75 %) but flags large
  purchases (53 % FPR) and legit bursts (30 %); it misses `app_scam` and
  slow drip. On IBM AML it is at the base rate (PR-AUC 0.002): laundering
  looks normal per transaction. Its weight must be justified by its
  contribution to the combined score (Phase 8), not alone. The artifact is a
  pickle: only load models trained here.
- **Impact:** `ml/anomaly.py`, `ml report.render_anomaly`, CLI
  `train-anomaly`, make targets, ARCHITECTURE §6.2.

### ADR-027: Read side: one store per query; benchmarks on the production code path
- **Decision:** The API reads each store through `fraudx.storage.readers` with
  queries its data model serves in one step: Cassandra single-partition reads
  (`transactions_by_id`, one customer-day), MongoDB `_id` lookup, Neo4j
  patterns anchored on one customer. There is no `GET /transactions` list: a
  global scan is not a Cassandra query; history is browsed per customer-day.
  Store clients are created on first request. The storage benchmark reuses the
  production schema, `HistoryWriter` and `HistoryReader` in an isolated
  `fraudx_bench` keyspace / `bench_transactions` collection / `fx:bench:` keys,
  dropped afterwards.
- **Reason:** The course is about choosing the right NoSQL model per access
  pattern; the read side is where that shows. Benchmarking the real code path
  (not a toy schema) makes the numbers meaningful for this system.
- **Advantages:** Latency independent of data volume; API starts while a
  store is down (`/health/ready` reports it); reproducible numbers (`make bench`).
- **Trade-offs:** The benchmark runs on one local Docker node, so it shows
  per-node costs, not Cassandra's horizontal write scaling. Measured
  (`docs/benchmarks/storage.md`, 20k transactions): batch writes MongoDB
  27.3k/s vs Cassandra 4.0k/s (two tables, Python driver, 64 in flight);
  read by id p50 MongoDB 0.15 ms vs Cassandra 0.67 ms; customer-day read
  0.31 vs 0.50 ms; cache lookup Redis 0.08 ms vs MongoDB 0.15 ms; on disk
  Cassandra 2.75 MiB (two tables) vs MongoDB 2.12 MiB.
- **Impact:** `storage/readers.py`, `api/stores.py`, `api/routes/{transactions,customers}.py`,
  `bench.py`, `docs/benchmarks/storage.md`.

### ADR-026: Offline feature builds run on a disposable Redis
- **Decision:** Compose service `redis-offline` (profile `jobs`, port 6380,
  10 GB, `noeviction`, no persistence) hosts all training feature builds.
  `OnlineFeatureStore(cache_results=False)` skips the per-transaction result
  cache offline.
- **Reason:** PaySim alone means 6.3 M customers; the live Redis has 512 MB
  with `volatile-lru`, so a build could evict live feature state (a known
  issue since Phase 5). The result cache exists only for redelivery, which
  can't happen offline, and it would double memory.
- **Advantages:** Live state is safe; an over-sized build fails loudly
  (`noeviction`) instead of producing wrong features; the code path is
  identical to live (same Lua).
- **Trade-offs:** One more container (started on demand by `make features`
  / `make paysim`).

### ADR-025: Public-dataset pipeline: explicit stages, auditable rules, nothing invented
- **Decision:** `fraudx.datasets` runs profile → clean → canonicalize per
  dataset, then the existing feature and training code. Cleaning rules are
  named, ordered, and record rationale plus rows *and positive labels*
  affected (`drop_rows`, `drop_column`, `transform`, `flag`). A Markdown
  report (`docs/data/<name>.md`) is generated from the artefacts. Polars does
  the batch processing. Fields a source lacks stay empty: this replaces the
  earlier hash-based device/IP enrichment of PaySim.
- **Reason:** The user asked for clear, organised cleaning and feature
  engineering. For an academic project, every number must be reproducible
  and every data decision justified by measured evidence. Inventing
  devices/IPs for PaySim would create features that look meaningful but
  carry no information.
- **Advantages:** Reproducible (`make paysim`, `make ibm-aml`); auditable
  (label impact of every rule); stages re-runnable independently; the same
  canonical layout feeds replay, feature building and training.
- **Trade-offs:** Polars is a new dependency (one library, no pandas). The
  canonical JSONL is large (PaySim 2.4 GB) but keeps one format for every
  downstream tool.

### ADR-024: Public datasets: PaySim for the supervised benchmark, IBM AML for the graph (implemented)
- **Context:** The user wants real-world value beyond our generator. There
  is no openly licensed dataset of *real* bank transactions with customer,
  device and account identities (privacy). The options are:
  - [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1): 6.3 M
    transactions, synthetic but simulated from aggregated real mobile-money
    logs of an African operator (Lopez-Rojas et al., EMSS 2016). It is the
    spec's dataset. It has no device/IP data, and `nameOrig` is almost
    unique per row.
  - IBM AML ([Altman et al., NeurIPS 2023](https://proceedings.neurips.cc/paper_files/paper/2023/hash/5f38404edff6f3f642d6fa5892479c42-Abstract.html)):
    synthetic transactions between individuals, companies and banks,
    calibrated to real data, with labelled laundering patterns (fan-in,
    fan-out, cycles…). HI-Small has **0.10 %** illicit transactions (5,177
    of 5,078,345, measured; an earlier note wrongly said about 2 %). It is
    graph-native.
  - [IEEE-CIS / Vesta](https://ieee-dataport.org/documents/ieee-cis-fraud-detection):
    real e-commerce data, but anonymised and released under Kaggle
    *competition* rules (use for the competition). Not suitable as the core
    of a published project.
- **Proposal:** Use three data sources, each for what it proves:
  - the generator v2 (end-to-end demo, controlled scenarios, per-detector
    contribution);
  - PaySim (supervised benchmark on simulator data calibrated on real logs,
    with the balance-leakage ablation);
  - IBM AML (graph detector benchmark on calibrated laundering patterns).
  Loaders map each into `TransactionEvent`. IBM AML needs new payment types
  and currencies: an additive schema change.
- **Status:** Implemented. Downloaded with the user's Kaggle token
  (`~/.kaggle/access_token`, outside the repo). Licences confirmed from
  Kaggle metadata: PaySim **CC BY-SA 4.0**, IBM AML **CDLA-Sharing-1.0**.
  Datasets stay git-ignored. IEEE-CIS is not used. See ADR-025 and
  `docs/data/`.

### ADR-023: Synthetic metrics are near-perfect, so the generator needs harder cases (implemented: generator v2)
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
- **Status:** Implemented. Model metrics on v2 (test PR-AUC 0.775,
  app_scam recall 0) now leave measurable room for the anomaly and graph
  detectors.

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

1. IBM AML graph features + graph model (build running, ~1.5–2 h): compare
   with the supervised baseline 0.147.
2. Possible next steps: a learned combiner for laundering-type data (on IBM
   AML stacking beats the weighted engine, 0.253 vs 0.197); multi-hop graph
   features (cycles, scatter-gather); scale-out runs (`--scale scorer=6`).
3. Feedback loop (later): a confirmed-fraud decision could set `fraud_flag` in
   Neo4j and feed a `transactions.labels` topic for retraining.

## Lessons learned

* Inserting a **non-frozen** Cassandra collection writes a tombstone first
  (to replace the old value). Use `frozen<...>` for values written whole, and
  bind empty collections as UNSET.
* `git stash -u` during a long background job removes files a *new* process
  of that job may import; verify a commit in a separate `git worktree`.
* `consume(n, timeout)` blocks until n messages **or** the timeout: a 1 s
  timeout added up to a second per batch stage at low traffic. Measure end
  to end (`make bench-stream`) before trusting per-stage numbers.
* Prometheus reads a bind-mounted config only at start; `docker compose up`
  doesn't restart it when the file changes (the scorer was unscraped for a
  while). `make up` now sends SIGHUP to reload it.
* Headless Chrome's `--virtual-time-budget`/`--timeout` stall or starve pages
  with an open SSE stream; drive Chrome over the DevTools protocol and wait
  real time to screenshot a live page.
* `pkill -f <pattern>` also matches the shell that runs it when the pattern
  appears in its own command line.
* A tool that selects "the latest model" must filter by model name once
  several model kinds share `models/` (the dataset report crashed on an
  Isolation Forest artifact).

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
* A test assertion like `... or True` or `... or len(x) > 1` is
  vacuous. Assert what the code actually guarantees (e.g. "the customer's
  primary device", not "a device the stream has already seen").
* Near-perfect metrics on synthetic data are a property of the generator,
  not a success of the model. Always break results down by scenario and ask
  what makes them separable.
* **Profile before cleaning.** Every rule in `fraudx.datasets` came from a
  measured number. Several "obvious" rules would have been wrong:
  - FX had to be derived *before* dropping self-transfers, because 96 % of
    USD conversions are self-transfers (a guard raised instead of silently
    producing wrong amounts);
  - a "sparse day" defined as < 10 % of the median day flagged one day,
    because half the days are sparse; the artefact is really an *inflated
    fraud share*, so flag that directly;
  - PaySim zero-amount rows are 100 % fraud, and IBM's post-simulation tail
    is 91 % laundering. Both are artefacts that would be learned.
* A categorical model input must hold small non-negative integer codes.
  Random floats in a categorical column make LightGBM overfit (toy test).
* Kaggle's newer CLI reads `~/.kaggle/access_token` (KGAT tokens). Kaggle
  metadata reports each dataset's licence; check it, don't assume.
* Prometheus must scrape `/metrics/` (trailing slash) because the metrics app
  is mounted as a sub-application.
