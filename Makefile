ENV_FILE ?= .env
COMPOSE_BASE := infra/compose/compose.base.yml
COMPOSE_DEV := infra/compose/compose.dev.yml
COMPOSE_CMD := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_BASE) -f $(COMPOSE_DEV)

LOCAL_ENV := POSTGRES_HOST=localhost MINIO_S3_ENDPOINT=http://localhost:9000 ICEBERG_REST_URI=http://localhost:8181

.PHONY: up down build-airflow bootstrap lint test pre-commit run-register

build-airflow:
	$(COMPOSE_CMD) build airflow-webserver airflow-scheduler

up:
	@if [ ! -f $(ENV_FILE) ]; then echo "Missing $(ENV_FILE). Copy .env.example first."; exit 1; fi
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

## Inspect all Iceberg table contents (Bronze + Silver register tables)
inspect-tables:
	$(LOCAL_ENV) poetry run python check_tables.py

.PHONY: dag-validate
dag-validate:
	docker exec compose-airflow-scheduler-1 \
		airflow dags list-import-errors
	docker exec compose-airflow-scheduler-1 \
		airflow dags list | grep -E "register|archives|silver|bronze"
