# FraudX developer commands. `make help` lists them.
.DEFAULT_GOAL := help
COMPOSE := docker compose
INFRA := kafka kafka-init mongodb cassandra cassandra-init redis neo4j neo4j-init

.PHONY: web web-build data data-train features train tune-risk offline-redis offline-neo4j download-datasets paysim ibm-aml bench replay tail help env install lint format test test-integration check up infra down clean ps logs build api topics cqlsh mongosh redis-cli cypher

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from .env.example (never overwrites)
	@test -f .env && echo ".env already exists" || (cp .env.example .env && echo "created .env - change the passwords")

install: ## Install Python dependencies (incl. dev tools) with uv
	uv sync

lint: ## Lint and check formatting
	uv run ruff check .
	uv run ruff format --check .

format: ## Auto-format and fix lint
	uv run ruff format .
	uv run ruff check --fix .

test: ## Unit tests (no services needed)
	uv run pytest

test-integration: ## Integration tests against the running stack (make up)
	set -a && . ./.env && set +a && uv run pytest -m integration

check: lint test ## Lint + unit tests (run before every commit)

data: ## Generate the synthetic dataset into data/generated (ARGS="--customers 5000")
	uv run python -m fraudx.ingestion generate $(ARGS)

data-train: ## Generate the (larger, different-seed) training dataset into data/training/synthetic
	uv run python -m fraudx.ingestion generate --out data/training/synthetic --seed 1337 --customers 5000 --days 30 --rings 12

offline-redis: ## Start the disposable Redis used for offline feature builds
	$(COMPOSE) --profile jobs up -d --wait redis-offline

offline-neo4j: ## Start the disposable Neo4j used for offline graph-feature builds
	$(COMPOSE) --profile jobs up -d --wait neo4j-offline

features: offline-redis offline-neo4j ## Build transaction + graph features (online code) for D=<dataset dir>
	uv run python -m fraudx.ml build-features --dataset $(or $(D),data/training/synthetic)
	uv run python -m fraudx.ml build-graph-features --dataset $(or $(D),data/training/synthetic)

train: offline-redis offline-neo4j ## Train + evaluate the fraud, anomaly and graph models (holdout: demo dataset), register and activate them
	uv run python -m fraudx.ml build-features --dataset data/generated
	uv run python -m fraudx.ml build-graph-features --dataset data/generated
	uv run python -m fraudx.ml train --dataset data/training/synthetic --holdout data/generated --activate $(ARGS)
	uv run python -m fraudx.ml train-anomaly --dataset data/training/synthetic --holdout data/generated --activate
	uv run python -m fraudx.ml train --graph --dataset data/training/synthetic --holdout data/generated --activate
	$(MAKE) tune-risk

tune-risk: ## Tune risk weights/thresholds on validation with the active models -> config/risk.yaml + report
	set -a && . ./.env && set +a && uv run python -m fraudx.ml tune-risk --dataset data/training/synthetic --holdout data/generated --write-config

download-datasets: ## Download PaySim + IBM AML HI-Small from Kaggle (token in ~/.kaggle/access_token)
	uvx kaggle datasets download ealaxi/paysim1 -p data/raw/paysim --unzip
	for f in HI-Small_Trans.csv HI-Small_Patterns.txt HI-Small_accounts.csv; do \
	  uvx kaggle datasets download ealtman2019/ibm-transactions-for-anti-money-laundering-aml -f $$f -p data/raw/ibm_aml; done

paysim: offline-redis ## PaySim: profile -> clean -> canonicalize -> features -> train -> report
	uv run python -m fraudx.datasets paysim --stages profile,clean,canonical
	uv run python -m fraudx.ml build-features --dataset data/processed/paysim
	uv run python -m fraudx.ml train --dataset data/processed/paysim --register
	uv run python -m fraudx.ml train-anomaly --dataset data/processed/paysim --register
	uv run python -m fraudx.datasets paysim --stages report

ibm-aml: offline-redis offline-neo4j ## IBM AML: profile -> clean -> canonicalize -> features -> train -> report
	uv run python -m fraudx.datasets ibm_aml --stages profile,clean,canonical
	uv run python -m fraudx.ml build-features --dataset data/processed/ibm_aml
	uv run python -m fraudx.ml train --dataset data/processed/ibm_aml --register
	uv run python -m fraudx.ml train-anomaly --dataset data/processed/ibm_aml --register
	uv run python -m fraudx.ml build-graph-features --dataset data/processed/ibm_aml
	uv run python -m fraudx.ml train --graph --dataset data/processed/ibm_aml --register
	uv run python -m fraudx.datasets ibm_aml --stages report

bench: ## Storage benchmarks (MongoDB vs Cassandra, MongoDB vs Redis) -> docs/benchmarks/storage.md
	set -a && . ./.env && set +a && uv run python -m fraudx.bench $(ARGS)

web: ## Dashboard dev server on :5173 with hot reload (proxies /api to the API on API_PORT)
	cd frontend && npm install --no-audit --no-fund && FRAUDX_API=http://localhost:$${API_PORT:-8000} npm run dev

web-build: ## Type-check and build the dashboard (frontend/dist)
	cd frontend && npm ci --no-audit --no-fund && npm run build

replay: ## Publish data/generated to transactions.raw (ARGS="--rate 500 --rebase-time")
	uv run python -m fraudx.ingestion replay $(ARGS)

tail: ## Print messages from a topic (T=transactions.raw ARGS="--from-beginning -n 5")
	uv run python -m fraudx.messaging tail $(or $(T),transactions.raw) $(ARGS)

build: ## Build the application image
	$(COMPOSE) build

up: env ## Start the full platform and wait until healthy
	$(COMPOSE) up -d --build --wait
	@# Prometheus reads its bind-mounted config only at start: reload it in place.
	$(COMPOSE) kill -s SIGHUP prometheus

infra: env ## Start only Kafka + the four databases (+ provisioning)
	$(COMPOSE) up -d --wait $(INFRA)

api: ## Run the API on the host with auto-reload (needs `make infra`)
	uv run uvicorn fraudx.api.app:app --reload --port 8000

down: ## Stop the platform (keeps data volumes)
	$(COMPOSE) down

clean: ## Stop the platform and DELETE all data volumes
	$(COMPOSE) down -v --remove-orphans

ps: ## Service status
	$(COMPOSE) ps -a

logs: ## Follow logs (S=service to filter)
	$(COMPOSE) logs -f --tail=100 $(S)

topics: ## Describe Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe

cqlsh: ## Open a CQL shell
	$(COMPOSE) exec cassandra cqlsh -k fraudx

mongosh: ## Open a Mongo shell as the app user
	$(COMPOSE) exec mongodb sh -c 'mongosh -u "$$MONGO_APP_USER" -p "$$MONGO_APP_PASSWORD" --authenticationDatabase "$$MONGO_DB" "$$MONGO_DB"'

redis-cli: ## Open a Redis shell
	$(COMPOSE) exec redis redis-cli

cypher: ## Open a Cypher shell
	$(COMPOSE) exec neo4j sh -c 'cypher-shell -u neo4j -p "$${NEO4J_AUTH#neo4j/}"'
