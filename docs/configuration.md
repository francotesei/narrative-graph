# Configuration Guide

## Overview

System configuration is managed through `configs/config.yaml` and environment variables.

## Configuration File

### Location

```
configs/config.yaml
```

### Complete Structure

```yaml
# File paths
paths:
  data_dir: "data"
  outputs_dir: "outputs"
  logs_dir: "logs"

# Neo4j connection
neo4j:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "password"
  database: "neo4j"

# Logging
logging:
  level: "INFO"
  format: "json"
  file: "logs/app.log"

# Ingestion
ingestion:
  batch_size: 1000
  skip_invalid: true
  dead_letter_enabled: true

# Features
features:
  entity_extractor: "spacy"
  spacy_model: "en_core_web_sm"

# Embeddings and Clustering
narratives:
  embedding_provider: "sentence-transformers"
  embedding_model: "all-MiniLM-L6-v2"
  clustering_algorithm: "hdbscan"
  min_cluster_size: 5
  min_samples: 3

# Coordination
coordination:
  time_window_minutes: 60
  similarity_threshold: 0.85
  min_group_size: 3
  text_similarity_weight: 0.5
  shared_domain_weight: 0.3
  shared_hashtag_weight: 0.2

# Risk Scoring
risk:
  weights:
    velocity: 0.25
    coordination_density: 0.30
    bot_score: 0.20
    foreign_domain_ratio: 0.15
    toxicity: 0.10
  thresholds:
    low: 0.3
    medium: 0.6
    high: 0.8
  foreign_tlds:
    - ".ru"
    - ".cn"
    - ".ir"

# Explanations
explain:
  use_llm: false
  llm_model: "gpt-4o-mini"
  max_tokens: 500

# API
api:
  host: "0.0.0.0"
  port: 8000
  debug: false

# UI
ui:
  title: "Narrative Graph Intelligence"
  theme: "dark"
```

## Environment Variables

Environment variables take priority over `config.yaml`.

### Supported Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NEO4J_URI` | Neo4j connection URI | `bolt://localhost:7687` |
| `NEO4J_USER` | Neo4j user | `neo4j` |
| `NEO4J_PASSWORD` | Neo4j password | `password` |
| `OPENAI_API_KEY` | OpenAI API key | - |
| `LOG_LEVEL` | Logging level | `INFO` |
| `DATA_DIR` | Data directory | `data` |
| `OUTPUTS_DIR` | Outputs directory | `outputs` |

### Example .env

```bash
# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=my_secure_password

# OpenAI (optional)
OPENAI_API_KEY=sk-...

# Logging
LOG_LEVEL=DEBUG
```

## Module Configuration

### Neo4j

```yaml
neo4j:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "password"
  database: "neo4j"
  max_connection_pool_size: 50
  connection_timeout: 30
```

### Embeddings

**sentence-transformers (local):**
```yaml
narratives:
  embedding_provider: "sentence-transformers"
  embedding_model: "all-MiniLM-L6-v2"
```

**OpenAI (cloud):**
```yaml
narratives:
  embedding_provider: "openai"
  openai_model: "text-embedding-3-small"
```

### Clustering

**HDBSCAN:**
```yaml
narratives:
  clustering_algorithm: "hdbscan"
  min_cluster_size: 5
  min_samples: 3
  cluster_selection_epsilon: 0.0
```

**KMeans:**
```yaml
narratives:
  clustering_algorithm: "kmeans"
  n_clusters: 10
  random_state: 42
```

### Entities

**spaCy:**
```yaml
features:
  entity_extractor: "spacy"
  spacy_model: "en_core_web_sm"
```

**LLM:**
```yaml
features:
  entity_extractor: "llm"
  llm_model: "gpt-4o-mini"
```

## Configuration Profiles

### Development

```yaml
logging:
  level: "DEBUG"
  format: "text"

api:
  debug: true

neo4j:
  uri: "bolt://localhost:7687"
```

### Production

```yaml
logging:
  level: "INFO"
  format: "json"

api:
  debug: false

neo4j:
  uri: "bolt://neo4j-cluster:7687"
  max_connection_pool_size: 100
```

### Testing

```yaml
logging:
  level: "WARNING"

neo4j:
  uri: "bolt://localhost:7688"
  database: "test"
```

## Validation

Configuration is validated at startup using Pydantic:

```python
from narrative_graph.config import load_config

config = load_config()  # Validates automatically
print(config.neo4j.uri)
```

Validation errors are reported clearly:

```
ValidationError: 1 validation error for Settings
neo4j -> password
  field required (type=value_error.missing)
```
