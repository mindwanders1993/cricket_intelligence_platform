ENV_FILE ?= .env
COMPOSE_BASE := infra/compose/compose.base.yml
COMPOSE_DEV := infra/compose/compose.dev.yml
COMPOSE_CMD := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_BASE) -f $(COMPOSE_DEV)

.PHONY: up down bootstrap lint test pre-commit

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
	docker compose --env-file $(ENV_FILE) -f infra/compose/compose.base.yml exec -T postgres \
		psql -U $${POSTGRES_USER:-cricket_user} -d $${POSTGRES_DB:-cricket_platform} \
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
