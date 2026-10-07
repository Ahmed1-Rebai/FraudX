# FraudX developer commands. `make help` lists them.
.DEFAULT_GOAL := help
COMPOSE := docker compose
INFRA := kafka kafka-init mongodb cassandra cassandra-init redis neo4j neo4j-init

.PHONY: data data-paysim help env install lint format test test-integration check up infra down clean ps logs build api topics cqlsh mongosh redis-cli cypher

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

data-paysim: ## Convert data/raw/PS_log.csv (Kaggle PaySim) into data/processed/paysim
	uv run python -m fraudx.ingestion paysim $(ARGS)

build: ## Build the application image
	$(COMPOSE) build

up: env ## Start the full platform and wait until healthy
	$(COMPOSE) up -d --build --wait

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
