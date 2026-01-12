# Command Reference

## Quick Reference

```bash
# Quick setup
make quickstart

# Complete pipeline
make run-all FILE=data/sample.jsonl

# Start UI
make ui
```

---

## Available Commands

### Setup & Infrastructure

| Command | Description |
|---------|-------------|
| `make install` | Install dependencies with uv |
| `make dev-install` | Install with dev dependencies |
| `make neo4j-up` | Start Neo4j with Docker |
| `make neo4j-down` | Stop Neo4j |
| `make neo4j-logs` | View Neo4j logs |
| `make neo4j-shell` | Open Cypher shell |
| `make db-init` | Initialize database schema |
| `make quickstart` | Full setup (install + neo4j + db-init) |

### Pipeline

| Command | Description | Example |
|---------|-------------|---------|
| `make ingest FILE=<path>` | Ingest data from file | `make ingest FILE=data/sample.jsonl` |
| `make enrich RUN_ID=<id>` | Enrich posts with features | `make enrich RUN_ID=run_abc123` |
| `make cluster RUN_ID=<id>` | Cluster into narratives | `make cluster RUN_ID=run_abc123` |
| `make build-graph RUN_ID=<id>` | Build Neo4j graph | `make build-graph RUN_ID=run_abc123` |
| `make detect-coord RUN_ID=<id>` | Detect coordination | `make detect-coord RUN_ID=run_abc123` |
| `make score-risk RUN_ID=<id>` | Calculate risk scores | `make score-risk RUN_ID=run_abc123` |
| `make explain RUN_ID=<id>` | Generate explanations | `make explain RUN_ID=run_abc123` |
| `make run-all FILE=<path>` | Run complete pipeline | `make run-all FILE=data/sample.jsonl` |

### Applications

| Command | Description | URL |
|---------|-------------|-----|
| `make ui` | Start Streamlit UI | http://localhost:8501 |
| `make api` | Start FastAPI (dev) | http://localhost:8000 |
| `make api-prod` | Start FastAPI (prod) | http://localhost:8000 |

### Development

| Command | Description |
|---------|-------------|
| `make test` | Run tests |
| `make test-cov` | Run tests with coverage |
| `make test-fast` | Run fast tests (no Neo4j) |
| `make lint` | Run linter (ruff) |
| `make lint-fix` | Fix lint errors |
| `make format` | Format code |
| `make format-check` | Check format |
| `make typecheck` | Type check (mypy) |
| `make check` | Run lint + typecheck |

### Cleanup

| Command | Description |
|---------|-------------|
| `make clean` | Clean temporary files |
| `make clean-outputs` | Clean pipeline outputs |
| `make clean-neo4j` | Clean Neo4j data |
| `make clean-all` | Clean everything |

---

## Direct CLI Commands

You can also use the CLI directly with `uv run`:

```bash
# Show help
uv run narrative-graph --help

# Individual commands
uv run narrative-graph ingest data/sample.jsonl
uv run narrative-graph enrich run_abc123
uv run narrative-graph cluster run_abc123
uv run narrative-graph build-graph run_abc123
uv run narrative-graph detect-coordination run_abc123
uv run narrative-graph score-risk run_abc123
uv run narrative-graph explain run_abc123
uv run narrative-graph run-all data/sample.jsonl

# Initialize DB
uv run narrative-graph db-init
```

---

## Common Workflows

### 1. First Installation

```bash
# Clone repo
git clone <repo-url>
cd narrative-graph

# Full setup
make quickstart
```

### 2. Run Analysis

```bash
# Ensure Neo4j is running
make neo4j-up

# Run complete pipeline
make run-all FILE=data/my_data.jsonl

# View results
make ui
```

### 3. Development

```bash
# Install dev dependencies
make dev-install

# Make changes...

# Verify code
make check

# Run tests
make test

# Format
make format
```

### 4. Step-by-Step Pipeline

```bash
# 1. Ingest data
make ingest FILE=data/sample.jsonl
# Output: run_id (e.g., run_abc123)

# 2. Enrich
make enrich RUN_ID=run_abc123

# 3. Cluster
make cluster RUN_ID=run_abc123

# 4. Build graph
make build-graph RUN_ID=run_abc123

# 5. Detect coordination
make detect-coord RUN_ID=run_abc123

# 6. Calculate risk
make score-risk RUN_ID=run_abc123

# 7. Generate explanations
make explain RUN_ID=run_abc123
```

### 5. Debugging

```bash
# View Neo4j logs
make neo4j-logs

# Open Cypher shell
make neo4j-shell

# In the shell:
MATCH (n) RETURN labels(n), count(n);
```

---

## Environment Variables

You can configure behavior with environment variables:

```bash
# Change log level
LOG_LEVEL=DEBUG make run-all FILE=data/sample.jsonl

# Use alternative configuration
CONFIG_PATH=configs/prod.yaml make run-all FILE=data/sample.jsonl

# Specify Neo4j credentials
NEO4J_PASSWORD=my_password make db-init
```

---

## Troubleshooting

### Neo4j won't start

```bash
# View logs
make neo4j-logs

# Restart
make neo4j-down
make neo4j-up
```

### Neo4j connection error

```bash
# Verify it's running
docker ps | grep neo4j

# Check port
curl http://localhost:7474
```

### Tests fail

```bash
# Run tests without Neo4j
make test-fast

# View detailed output
uv run pytest tests/ -v -s
```

### Clean and start fresh

```bash
make clean-all
make neo4j-down
docker volume rm narrative-graph_neo4j_data
make quickstart
```
