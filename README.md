# Narrative Graph Intelligence MVP

A system for analyzing narratives in social media data, detecting coordinated behavior, and assessing risks.

## Features

- **Data Ingestion**: Load and normalize data from JSONL/CSV
- **Feature Extraction**: Extract URLs, hashtags, mentions, and entities (spaCy NER)
- **Narrative Detection**: Cluster posts into narratives using semantic embeddings (HDBSCAN)
- **Graph Building**: Build a knowledge graph in Neo4j
- **Coordination Detection**: Detect coordinated behavior via text similarity and temporal patterns
- **Risk Scoring**: Calculate risk scores based on velocity, coordination, and toxicity
- **Explainability**: Generate explanations with LLM (OpenAI) or templates
- **UI**: Streamlit dashboard to explore results
- **API**: REST endpoints with FastAPI

## Quick Start

### Prerequisites

- Python 3.11+
- Docker and Docker Compose
- [uv](https://github.com/astral-sh/uv) package manager

### Full Setup

```bash
# Automatic setup (installs dependencies, starts Neo4j, initializes DB)
make quickstart

# Run pipeline with sample data
make run-all FILE=data/sample_large.jsonl

# Start UI
make ui
```

### Manual Setup

```bash
# Install dependencies
make install

# Start Neo4j
make neo4j-up

# Initialize database schema
make db-init

# Run pipeline
make run-all FILE=data/sample.jsonl

# Start UI (http://localhost:8501)
make ui

# Or start API (http://localhost:8000/docs)
make api
```

## Makefile Commands

### Setup & Infrastructure

| Command | Description |
|---------|-------------|
| `make install` | Install dependencies with uv |
| `make dev-install` | Install with dev dependencies |
| `make neo4j-up` | Start Neo4j with Docker |
| `make neo4j-down` | Stop Neo4j |
| `make neo4j-logs` | View Neo4j logs |
| `make db-init` | Initialize database schema |
| `make quickstart` | Full automatic setup |

### Pipeline

| Command | Description |
|---------|-------------|
| `make run-all FILE=<path>` | Run complete pipeline |
| `make ingest FILE=<path>` | Ingest data from file |
| `make enrich RUN_ID=<id>` | Enrich posts with features |
| `make cluster RUN_ID=<id>` | Cluster posts into narratives |
| `make build-graph RUN_ID=<id>` | Build Neo4j graph |
| `make detect-coord RUN_ID=<id>` | Detect coordination |
| `make score-risk RUN_ID=<id>` | Calculate risk scores |
| `make explain RUN_ID=<id>` | Generate explanations |

### Applications

| Command | Description |
|---------|-------------|
| `make ui` | Start Streamlit UI (port 8501) |
| `make api` | Start FastAPI server (port 8000) |
| `make api-prod` | Start API in production mode |

### Development

| Command | Description |
|---------|-------------|
| `make test` | Run tests |
| `make test-cov` | Run tests with coverage |
| `make lint` | Run linter (ruff) |
| `make format` | Format code |
| `make typecheck` | Run type checker (mypy) |
| `make check` | Run lint + typecheck |

### Utilities

| Command | Description |
|---------|-------------|
| `make clean` | Clean generated files |
| `make clean-outputs` | Clean pipeline outputs |
| `make clean-neo4j` | Remove Neo4j volumes |
| `make clean-all` | Clean everything |

## Data Format

Input in JSONL or CSV:

```json
{"id": "post_001", "timestamp": "2024-01-15T10:30:00Z", "platform": "twitter", "author_id": "user_001", "text": "Example post #hashtag https://example.com"}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique identifier |
| `timestamp` | string | Yes | ISO8601 timestamp |
| `platform` | string | Yes | Platform (twitter, reddit, news) |
| `author_id` | string | Yes | Author ID |
| `text` | string | Yes | Post content |
| `author_handle` | string | No | Handle/username |
| `lang` | string | No | Language code |
| `urls` | list | No | URLs in the post |
| `hashtags` | list | No | Hashtags |
| `mentions` | list | No | @mentions |

## Project Structure

```
narrative-graph/
├── src/narrative_graph/
│   ├── ingestion/      # Data loading and normalization
│   ├── features/       # Feature extraction
│   ├── narratives/     # Clustering and detection
│   ├── graph/          # Neo4j operations
│   ├── coordination/   # Coordination detection
│   ├── risk/           # Risk scoring
│   ├── explain/        # Explanation generation
│   ├── storage/        # Persistence (SQLite, Parquet)
│   ├── api/            # FastAPI endpoints
│   ├── ui/             # Streamlit app
│   └── cli/            # CLI commands
├── tests/              # Unit tests
├── configs/            # Configuration files
├── data/               # Sample data
├── outputs/            # Pipeline outputs
├── docs/               # Detailed documentation
└── Makefile            # Automation commands
```

## Documentation

Detailed documentation available in [`docs/`](docs/):

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | System architecture and diagrams |
| [Data Pipeline](docs/data-pipeline.md) | Detailed pipeline flow |
| [Graph Model](docs/graph-model.md) | Neo4j data model |
| [Coordination Detection](docs/coordination-detection.md) | Detection algorithms |
| [Risk Engine](docs/risk-engine.md) | Risk scoring engine |
| [API Reference](docs/api-reference.md) | REST API documentation |
| [Configuration](docs/configuration.md) | Configuration guide |
| [Commands](docs/commands.md) | Complete command reference |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `NEO4J_PASSWORD` | Neo4j password (default: `password`) |
| `OPENAI_API_KEY` | OpenAI API key (optional) |

## Access URLs

| Service | URL |
|---------|-----|
| Streamlit UI | http://localhost:8501 |
| FastAPI Docs | http://localhost:8000/docs |
| Neo4j Browser | http://localhost:7474 |

**Neo4j Credentials**: `neo4j` / `password`

## License

MIT License
