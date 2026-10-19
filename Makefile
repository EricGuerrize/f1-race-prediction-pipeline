.PHONY: setup lint test demo ingest docker-build clean help

PYTHON ?= python3
UV ?= uv
SEASON ?= 2024
ROUND ?=

help:
	@echo "Targets:"
	@echo "  make setup         - cria venv e instala deps (uv)"
	@echo "  make lint          - ruff check + format --check"
	@echo "  make test          - pytest"
	@echo "  make demo          - demo offline com data/sample"
	@echo "  make ingest        - ingere season (SEASON=2024 ROUND=1)"
	@echo "  make docker-build  - build da imagem Docker"
	@echo "  make clean         - remove artefatos locais de dados gerados"

setup:
	$(UV) sync --extra dev
	$(UV) run pre-commit install || true

lint:
	$(UV) run ruff check src tests
	$(UV) run ruff format --check src tests

fmt:
	$(UV) run ruff check --fix src tests
	$(UV) run ruff format src tests

test:
	$(UV) run pytest

demo:
	$(UV) run f1p demo

ingest:
ifeq ($(ROUND),)
	$(UV) run f1p ingest --season $(SEASON)
else
	$(UV) run f1p ingest --season $(SEASON) --round $(ROUND)
endif

docker-build:
	docker build -t f1-pipeline:local .

clean:
	rm -rf data/bronze data/warehouse.duckdb data/cache .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
