ENV_FILE ?= .env
COMPOSE_BASE := infra/compose/compose.base.yml
COMPOSE_DEV := infra/compose/compose.dev.yml
COMPOSE_CMD := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_BASE) -f $(COMPOSE_DEV)

LOCAL_ENV := POSTGRES_HOST=localhost MINIO_S3_ENDPOINT=http://localhost:9000 ICEBERG_REST_URI=http://localhost:8181

.PHONY: up down build-airflow bootstrap lint test pre-commit run-register duckdb-ui duckdb-stop nuke rebuild provision-metabase refresh-gold

build-airflow:
	$(COMPOSE_CMD) build airflow-webserver airflow-scheduler

up:
	@if [ ! -f $(ENV_FILE) ]; then echo "Missing $(ENV_FILE). Copy .env.example first."; exit 1; fi
	@if [ "$(shell docker images -q cricket-airflow:latest 2>/dev/null)" = "" ]; then \
		echo "Custom Airflow image not found. Running build..."; \
		$(MAKE) build-airflow; \
	fi
	$(COMPOSE_CMD) up -d

down:
	$(COMPOSE_CMD) down

bootstrap:
	@echo "Running bootstrap scripts (buckets, control schema)..."
	@bash infra/bootstrap/create-buckets.sh
	@$(MAKE) bootstrap-db

## Run PostgreSQL control schema DDL (idempotent)
bootstrap-db:
	@echo "Running init-metastore.sql against PostgreSQL..."
	@set -a && . $(ENV_FILE) && set +a && \
	docker compose --env-file $(ENV_FILE) -f infra/compose/compose.base.yml exec -T postgres \
		psql -U $${POSTGRES_USER:-postgres} -d $${POSTGRES_DB:-cricket_platform} \
		-f /dev/stdin < infra/bootstrap/init-metastore.sql
	@echo "✓ Control schema ready"

## Run full bootstrap: MinIO buckets + PostgreSQL control schema
bootstrap-all: bootstrap bootstrap-db
	@echo "✓ Full bootstrap complete — MinIO + PostgreSQL ready"

lint:
	poetry run ruff check .
	poetry run black --check .
	poetry run isort --check-only .

test:
	poetry run pytest

pre-commit:
	poetry run pre-commit run --all-files

## Run register ingestion pipeline locally (overrides Docker-internal hostnames)
run-register:
	$(LOCAL_ENV) poetry run python -m cip.ingestion.jobs.ingest_people_and_names $(ARGS)

## Launch DuckDB built-in web UI at http://localhost:4213.
## NOTE: UI holds a write lock on the DB file. Stop it with `make duckdb-stop`
## (or Ctrl-C this process) before triggering the dag_run_gold_dbt_models DAG,
## otherwise the DAG's refresh_duckdb_views task will fail with a lock error.
duckdb-ui:
	@if ! command -v duckdb &>/dev/null; then \
		echo "Installing DuckDB CLI via Homebrew..."; \
		brew install duckdb; \
	fi
	@echo "Opening DuckDB UI at http://localhost:4213 (Ctrl-C to stop)"
	duckdb -ui storage/duckdb/cricket.duckdb

## Stop any running DuckDB UI process (releases the DB file lock for the DAG)
duckdb-stop:
	@pkill -f "duckdb .*-ui" 2>/dev/null && echo "DuckDB UI stopped" || echo "No DuckDB UI running"

## Inspect all Iceberg table contents (Bronze + Silver register tables)
inspect-tables:
	$(LOCAL_ENV) poetry run python check_tables.py

.PHONY: dag-validate
dag-validate:
	docker exec compose-airflow-scheduler-1 \
		airflow dags list-import-errors
	docker exec compose-airflow-scheduler-1 \
		airflow dags list | grep -E "ingest|silver|gold"

## Full teardown: stop UI, drop containers + volumes, clear host-side state.
nuke:
	-@$(MAKE) duckdb-stop
	$(COMPOSE_CMD) down -v --remove-orphans
	rm -f storage/duckdb/cricket.duckdb
	rm -rf models/dbt/target models/dbt/dbt_packages models/dbt/logs
	@echo "✓ Teardown complete — volumes dropped, host state cleared"

## Full bring-up: build Airflow image, start services, bootstrap MinIO + Postgres, validate DAGs.
rebuild:
	@test -f .env || (echo "Missing .env — copy from .env.example first" && exit 1)
	$(MAKE) build-airflow
	$(MAKE) up
	@echo "Waiting 40s for healthchecks..."
	@sleep 40
	$(MAKE) bootstrap
	$(MAKE) dag-validate
	@echo "✓ Rebuild complete — Airflow at http://localhost:8080"

## Provision Metabase dashboards (handles first-boot setup automatically after a volume wipe).
provision-metabase:
	poetry run python scripts/provision_metabase_dashboards.py

## Release DuckDB locks, trigger Gold DAG, wait for it, restart Metabase.
refresh-gold:
	-@$(MAKE) duckdb-stop
	@echo "Stopping Metabase to release DuckDB read lock..."
	@docker stop compose-metabase-1 >/dev/null 2>&1 || true
	@echo "Triggering dag_run_gold_dbt_models..."
	@docker exec compose-airflow-scheduler-1 airflow dags trigger dag_run_gold_dbt_models
	@echo "Waiting for Gold DAG to complete (follow progress at http://localhost:8080)..."
	@while true; do \
		state=$$(docker exec compose-airflow-scheduler-1 airflow dags list-runs -d dag_run_gold_dbt_models --output json 2>/dev/null | python3 -c "import json,sys; runs=json.load(sys.stdin); print(runs[0]['state'] if runs else 'pending')" 2>/dev/null || echo "pending"); \
		echo "  Gold DAG state: $$state"; \
		if [ "$$state" = "success" ]; then break; fi; \
		if [ "$$state" = "failed" ]; then echo "✗ Gold DAG FAILED — check Airflow logs"; exit 1; fi; \
		sleep 15; \
	done
	@echo "Restarting Metabase..."
	@docker start compose-metabase-1 >/dev/null
	@until curl -fsS http://localhost:3000/api/health >/dev/null 2>&1; do sleep 5; done
	@echo "✓ Gold refreshed, Metabase healthy"

# ── Dashboard (Observable Framework) ──
.PHONY: dashboard-install dashboard-dev dashboard-build dashboard-clean

## Install dashboard npm dependencies (first-time setup).
dashboard-install:
	cd dashboard && npm install

## Run dashboard dev server on http://localhost:3030.
## Requires `poetry shell` active (or `poetry env activate`) so Python data
## loaders can `import duckdb`. Coexists with Metabase (DuckDB read-only).
dashboard-dev:
	cd dashboard && npm run dev

## Build static dashboard site to dashboard/dist/.
dashboard-build:
	cd dashboard && npm run build

## Remove dashboard build artifacts and caches.
dashboard-clean:
	cd dashboard && rm -rf dist node_modules src/.observablehq/cache
