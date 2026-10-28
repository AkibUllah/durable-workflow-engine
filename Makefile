.DEFAULT_GOAL := help
VENV ?= .venv
PY := $(VENV)/bin/python

.PHONY: help venv install dev worker test cov lint fmt bench openapi docker-up docker-down clean

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

venv: ## Create virtualenv
	test -d $(VENV) || python3 -m venv $(VENV)

install: venv ## Install with dev extras
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -e ".[dev]"

dev: ## API + embedded workers with reload on :8000 (dashboard at /)
	$(VENV)/bin/uvicorn durable.asgi:app --reload --port 8000

worker: ## Standalone worker process
	$(VENV)/bin/durable worker --concurrency 4

test: ## Run tests
	$(PY) -m pytest

cov: ## Tests with coverage
	$(PY) -m pytest --cov=durable --cov-report=term-missing

lint: ## Ruff check + format check
	$(VENV)/bin/ruff check durable tests bench scripts
	$(VENV)/bin/ruff format --check durable tests bench scripts

fmt: ## Format + autofix
	$(VENV)/bin/ruff format durable tests bench scripts
	$(VENV)/bin/ruff check --fix durable tests bench scripts

bench: ## Throughput benchmark on SQLite
	$(PY) bench/bench.py

openapi: ## Export the OpenAPI document to openapi.json
	$(PY) scripts/export_openapi.py

docker-up: ## api + 2 workers + postgres
	docker compose up --build -d

docker-down: ## Stop stack
	docker compose down -v

clean: ## Remove caches and local DB
	rm -rf .pytest_cache .ruff_cache htmlcov .coverage durable.db*
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
