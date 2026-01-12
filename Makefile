# ============================================================================
# Narrative Graph Intelligence - Makefile
# ============================================================================

.PHONY: help install dev-install neo4j-up neo4j-down neo4j-logs db-init \
        ingest enrich cluster build-graph detect-coord score-risk explain run-all \
        ui api test lint format typecheck clean

# Default target
help:
	@echo "╔══════════════════════════════════════════════════════════════════════╗"
	@echo "║           Narrative Graph Intelligence - Commands                    ║"
	@echo "╚══════════════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  Setup & Infrastructure"
	@echo "  ──────────────────────"
	@echo "    make install        Install dependencies with uv"
	@echo "    make dev-install    Install with dev dependencies"
	@echo "    make neo4j-up       Start Neo4j with Docker"
	@echo "    make neo4j-down     Stop Neo4j"
	@echo "    make neo4j-logs     View Neo4j logs"
	@echo "    make db-init        Initialize database schema"
	@echo ""
	@echo "  Pipeline Commands"
	@echo "  ─────────────────"
	@echo "    make ingest FILE=<path>       Ingest data from file"
	@echo "    make enrich RUN_ID=<id>       Enrich posts with features"
	@echo "    make cluster RUN_ID=<id>      Cluster posts into narratives"
	@echo "    make build-graph RUN_ID=<id>  Build Neo4j graph"
	@echo "    make detect-coord RUN_ID=<id> Detect coordination"
	@echo "    make score-risk RUN_ID=<id>   Calculate risk scores"
	@echo "    make explain RUN_ID=<id>      Generate explanations"
	@echo "    make run-all FILE=<path>      Run complete pipeline"
	@echo ""
	@echo "  Applications"
	@echo "  ────────────"
	@echo "    make ui             Start Streamlit UI (port 8501)"
	@echo "    make api            Start FastAPI server (port 8000)"
	@echo ""
	@echo "  Development"
	@echo "  ───────────"
	@echo "    make test           Run tests"
	@echo "    make test-cov       Run tests with coverage"
	@echo "    make lint           Run linter (ruff)"
	@echo "    make format         Format code (ruff)"
	@echo "    make typecheck      Run type checker (mypy)"
	@echo "    make check          Run all checks (lint + typecheck)"
	@echo ""
	@echo "  Utilities"
	@echo "  ─────────"
	@echo "    make clean          Clean generated files"
	@echo "    make clean-outputs  Clean pipeline outputs"
	@echo "    make clean-all      Clean everything"
	@echo ""

# ============================================================================
# Setup & Infrastructure
# ============================================================================

install:
	uv sync

dev-install:
	uv sync --all-extras

neo4j-up:
	docker-compose up -d
	@echo "Waiting for Neo4j to be ready..."
	@sleep 10
	@echo "Neo4j is running at http://localhost:7474"

neo4j-down:
	docker-compose down

neo4j-logs:
	docker-compose logs -f neo4j

neo4j-shell:
	docker exec -it narrative-graph-neo4j cypher-shell -u neo4j -p password

db-init:
	uv run narrative-graph db-init

# ============================================================================
# Pipeline Commands
# ============================================================================

# Usage: make ingest FILE=data/sample.jsonl
ingest:
ifndef FILE
	$(error FILE is required. Usage: make ingest FILE=data/sample.jsonl)
endif
	uv run narrative-graph ingest $(FILE)

# Usage: make enrich RUN_ID=run_abc123
enrich:
ifndef RUN_ID
	$(error RUN_ID is required. Usage: make enrich RUN_ID=run_abc123)
endif
	uv run narrative-graph enrich $(RUN_ID)

# Usage: make cluster RUN_ID=run_abc123
cluster:
ifndef RUN_ID
	$(error RUN_ID is required. Usage: make cluster RUN_ID=run_abc123)
endif
	uv run narrative-graph cluster $(RUN_ID)

# Usage: make build-graph RUN_ID=run_abc123
build-graph:
ifndef RUN_ID
	$(error RUN_ID is required. Usage: make build-graph RUN_ID=run_abc123)
endif
	uv run narrative-graph build-graph $(RUN_ID)

# Usage: make detect-coord RUN_ID=run_abc123
detect-coord:
ifndef RUN_ID
	$(error RUN_ID is required. Usage: make detect-coord RUN_ID=run_abc123)
endif
	uv run narrative-graph detect-coordination $(RUN_ID)

# Usage: make score-risk RUN_ID=run_abc123
score-risk:
ifndef RUN_ID
	$(error RUN_ID is required. Usage: make score-risk RUN_ID=run_abc123)
endif
	uv run narrative-graph score-risk $(RUN_ID)

# Usage: make explain RUN_ID=run_abc123
explain:
ifndef RUN_ID
	$(error RUN_ID is required. Usage: make explain RUN_ID=run_abc123)
endif
	uv run narrative-graph explain $(RUN_ID)

# Usage: make run-all FILE=data/sample.jsonl
run-all:
ifndef FILE
	$(error FILE is required. Usage: make run-all FILE=data/sample.jsonl)
endif
	uv run narrative-graph run-all $(FILE)

# ============================================================================
# Applications
# ============================================================================

ui:
	uv run streamlit run src/narrative_graph/ui/app.py --server.port 8501

api:
	uv run uvicorn narrative_graph.api.main:app --host 0.0.0.0 --port 8000 --reload

api-prod:
	uv run uvicorn narrative_graph.api.main:app --host 0.0.0.0 --port 8000 --workers 4

# ============================================================================
# Development
# ============================================================================

test:
	uv run pytest tests/ -v

test-cov:
	uv run pytest tests/ -v --cov=src/narrative_graph --cov-report=html --cov-report=term

test-fast:
	uv run pytest tests/ -v -x --ignore=tests/test_graph.py

lint:
	uv run ruff check src/ tests/

lint-fix:
	uv run ruff check src/ tests/ --fix

format:
	uv run ruff format src/ tests/

format-check:
	uv run ruff format src/ tests/ --check

typecheck:
	uv run mypy src/narrative_graph

check: lint typecheck
	@echo "All checks passed!"

# ============================================================================
# Utilities
# ============================================================================

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf htmlcov/ .coverage 2>/dev/null || true

clean-outputs:
	rm -rf outputs/*

clean-neo4j:
	docker-compose down -v

clean-all: clean clean-outputs
	@echo "Cleaned all generated files"

# ============================================================================
# Quick Start
# ============================================================================

quickstart: install neo4j-up
	@echo "Waiting for Neo4j..."
	@sleep 15
	@make db-init
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════════════╗"
	@echo "║                    Setup Complete!                                   ║"
	@echo "╚══════════════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  Run the sample pipeline:"
	@echo "    make run-all FILE=data/sample.jsonl"
	@echo ""
	@echo "  Then start the UI:"
	@echo "    make ui"
	@echo ""

# ============================================================================
# Docker
# ============================================================================

docker-build:
	docker build -t narrative-graph .

docker-run:
	docker run -p 8000:8000 -p 8501:8501 narrative-graph
