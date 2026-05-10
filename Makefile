ENV_FILE ?= .env
COMPOSE_BASE := infra/compose/compose.base.yml
COMPOSE_DEV := infra/compose/compose.dev.yml
COMPOSE_CMD := docker compose -f $(COMPOSE_BASE) -f $(COMPOSE_DEV)

.PHONY: up down bootstrap lint test pre-commit

up:
	@if [ ! -f $(ENV_FILE) ]; then echo "Missing $(ENV_FILE). Copy .env.example first."; exit 1; fi
	$(COMPOSE_CMD) up -d

down:
	$(COMPOSE_CMD) down

bootstrap:
	@echo "Running bootstrap scripts (buckets, control schema)..."
	@bash infra/bootstrap/create-buckets.sh
	@psql "$$POSTGRES_CONNECTION_URI" -f infra/bootstrap/init-metastore.sql

lint:
	poetry run ruff check .
	poetry run black --check .
	poetry run isort --check-only .

test:
	poetry run pytest

pre-commit:
	poetry run pre-commit run --all-files
