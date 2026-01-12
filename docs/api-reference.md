# API Reference

## Overview

The REST API provides programmatic access to narrative analysis data and results.

## Base URL

```
http://localhost:8000
```

## Authentication

Currently the API does not require authentication. In production, it is recommended to implement OAuth2 or API keys.

## Endpoints

### Health Check

#### GET /health

Checks the API status and Neo4j connection.

**Response:**
```json
{
  "status": "healthy",
  "neo4j_connected": true,
  "version": "0.1.0"
}
```

**Status Codes:**
- `200`: API running
- `503`: Neo4j unavailable

---

### Runs

#### GET /runs

Lists pipeline executions.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| limit | int | 10 | Maximum number of results (max: 100) |

**Response:**
```json
{
  "runs": [
    {
      "run_id": "run_abc123def456",
      "status": "completed",
      "started_at": "2024-01-15T10:00:00",
      "completed_at": "2024-01-15T10:05:00",
      "input_file": "data/sample.jsonl"
    }
  ],
  "total": 1
}
```

#### GET /runs/{run_id}

Gets details of a specific execution.

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| run_id | string | Run identifier |

**Response:**
```json
{
  "run_id": "run_abc123def456",
  "status": "completed",
  "started_at": "2024-01-15T10:00:00",
  "completed_at": "2024-01-15T10:05:00",
  "input_file": "data/sample.jsonl"
}
```

**Status Codes:**
- `200`: Run found
- `404`: Run not found

---

### Narratives

#### GET /narratives

Lists detected narratives.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| run_id | string | required | Run identifier |
| risk_level | string | null | Filter by level (HIGH, MEDIUM, LOW) |
| min_size | int | 1 | Minimum narrative size |
| limit | int | 50 | Maximum results (max: 200) |
| offset | int | 0 | Offset for pagination |

**Response:**
```json
{
  "narratives": [
    {
      "id": "narrative_0001",
      "size": 50,
      "keywords": ["policy", "change", "government"],
      "top_domains": ["example.com", "news.org"],
      "top_hashtags": ["PolicyChange", "Breaking"],
      "risk_score": 0.72,
      "risk_level": "HIGH",
      "explanation": "This narrative consists of...",
      "author_count": 15
    }
  ],
  "total": 10
}
```

**Status Codes:**
- `200`: Success
- `404`: Run not found

#### GET /narratives/{narrative_id}

Gets details of a specific narrative.

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| narrative_id | string | Narrative identifier |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| run_id | string | required | Run identifier |

**Response:**
```json
{
  "id": "narrative_0001",
  "size": 50,
  "keywords": ["policy", "change", "government"],
  "top_domains": ["example.com", "news.org"],
  "top_hashtags": ["PolicyChange", "Breaking"],
  "risk_score": 0.72,
  "risk_level": "HIGH",
  "explanation": "This narrative consists of 50 posts discussing topics related to: policy, change, government...",
  "author_count": 15
}
```

**Status Codes:**
- `200`: Narrative found
- `404`: Narrative or run not found

---

### Coordination

#### GET /coordination

Lists detected coordination groups.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| run_id | string | required | Run identifier |
| min_score | float | 0.0 | Minimum coordination score |
| min_size | int | 2 | Minimum group size |
| limit | int | 50 | Maximum results (max: 200) |

**Response:**
```json
{
  "groups": [
    {
      "id": "coord_group_0001",
      "size": 5,
      "score": 0.85,
      "author_ids": ["user_001", "user_002", "user_003", "user_004", "user_005"],
      "narrative_ids": ["narrative_0001"],
      "evidence_summary": "Group of 5 authors with avg coordination score 0.85"
    }
  ],
  "total": 3
}
```

**Status Codes:**
- `200`: Success
- `404`: Coordination data not found

---

### Graph

#### GET /graph/stats

Gets Neo4j graph statistics.

**Response:**
```json
{
  "author_count": 100,
  "post_count": 500,
  "narrative_count": 10,
  "domain_count": 50,
  "hashtag_count": 30,
  "entity_count": 75,
  "rel_posted_count": 500,
  "rel_belongs_to_count": 450,
  "rel_links_to_count": 200,
  "rel_tagged_with_count": 300,
  "rel_mentions_count": 150,
  "top_narratives": [
    {"id": "narrative_0001", "size": 50, "post_count": 50}
  ]
}
```

**Status Codes:**
- `200`: Success
- `503`: Neo4j unavailable

#### GET /graph/narrative/{narrative_id}

Gets the subgraph of a narrative.

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| narrative_id | string | Narrative identifier |

**Response:**
```json
{
  "narrative_id": "narrative_0001",
  "nodes": [
    {
      "id": 1,
      "labels": ["Post"],
      "properties": {"id": "post_001", "platform": "twitter"}
    },
    {
      "id": 2,
      "labels": ["Author"],
      "properties": {"id": "user_001", "handle": "@user1"}
    }
  ],
  "edges": [
    {"source": 2, "target": 1, "type": "POSTED"}
  ],
  "node_count": 50,
  "edge_count": 100
}
```

**Status Codes:**
- `200`: Success
- `503`: Neo4j unavailable

---

## Data Models

### NarrativeResponse

```typescript
interface NarrativeResponse {
  id: string;
  size: number;
  keywords: string[];
  top_domains: string[];
  top_hashtags: string[];
  risk_score: number | null;
  risk_level: "HIGH" | "MEDIUM" | "LOW" | null;
  explanation: string | null;
  author_count: number;
}
```

### CoordinationGroupResponse

```typescript
interface CoordinationGroupResponse {
  id: string;
  size: number;
  score: number;
  author_ids: string[];
  narrative_ids: string[];
  evidence_summary: string;
}
```

### RunResponse

```typescript
interface RunResponse {
  run_id: string;
  status: "running" | "completed" | "failed";
  started_at: string;
  completed_at: string | null;
  input_file: string | null;
}
```

### HealthResponse

```typescript
interface HealthResponse {
  status: "healthy" | "degraded";
  neo4j_connected: boolean;
  version: string;
}
```

---

## Errors

### Error Format

```json
{
  "detail": "Error message"
}
```

### Common Error Codes

| Code | Description |
|------|-------------|
| 400 | Bad Request - Invalid parameters |
| 404 | Not Found - Resource not found |
| 500 | Internal Server Error - Server error |
| 503 | Service Unavailable - Neo4j unavailable |

---

## Usage Examples

### Python

```python
import httpx

# List high-risk narratives
response = httpx.get(
    "http://localhost:8000/narratives",
    params={
        "run_id": "run_abc123",
        "risk_level": "HIGH",
        "limit": 10
    }
)
narratives = response.json()["narratives"]

for n in narratives:
    print(f"{n['id']}: {n['risk_score']:.2f} - {n['keywords'][:3]}")
```

### cURL

```bash
# Health check
curl http://localhost:8000/health

# List runs
curl http://localhost:8000/runs

# Get narratives
curl "http://localhost:8000/narratives?run_id=run_abc123&risk_level=HIGH"

# Get coordination groups
curl "http://localhost:8000/coordination?run_id=run_abc123&min_score=0.8"

# Graph statistics
curl http://localhost:8000/graph/stats
```

### JavaScript

```javascript
// Using fetch
const response = await fetch(
  'http://localhost:8000/narratives?' + new URLSearchParams({
    run_id: 'run_abc123',
    risk_level: 'HIGH',
    limit: 10
  })
);

const data = await response.json();
console.log(`Found ${data.total} high-risk narratives`);
```

---

## Rate Limiting

Currently no rate limiting is implemented. In production, it is recommended:

- 100 requests/minute for read endpoints
- 10 requests/minute for endpoints that access Neo4j

---

## OpenAPI / Swagger

Interactive documentation is available at:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI JSON: `http://localhost:8000/openapi.json`
