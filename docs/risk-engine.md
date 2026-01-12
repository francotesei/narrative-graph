# Risk Engine

## Overview

The risk engine evaluates each detected narrative and assigns a risk score based on multiple weighted components.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              RISK ENGINE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    INPUT: Narrative + Posts + Groups                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    COMPONENT CALCULATORS                             │   │
│  │                                                                      │   │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐       │   │
│  │  │  Velocity  │ │   Coord    │ │    Bot     │ │  Foreign   │       │   │
│  │  │   Score    │ │  Density   │ │   Score    │ │  Domains   │       │   │
│  │  │   (0.25)   │ │   (0.30)   │ │   (0.20)   │ │   (0.15)   │       │   │
│  │  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘       │   │
│  │        │              │              │              │               │   │
│  │  ┌─────┴──────┐                                                    │   │
│  │  │  Toxicity  │                                                    │   │
│  │  │   Score    │                                                    │   │
│  │  │   (0.10)   │                                                    │   │
│  │  └─────┬──────┘                                                    │   │
│  │        │                                                            │   │
│  └────────┼────────────────────────────────────────────────────────────┘   │
│           │                                                                 │
│           ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      WEIGHTED AGGREGATION                            │   │
│  │                                                                      │   │
│  │  risk_score = Σ (weight_i × component_i)                            │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      CLASSIFICATION                                  │   │
│  │                                                                      │   │
│  │  score < 0.3  → LOW                                                 │   │
│  │  0.3 ≤ score < 0.6 → MEDIUM                                         │   │
│  │  score ≥ 0.6 → HIGH                                                 │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │              OUTPUT: NarrativeRisk (score, level, reasons)           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Risk Components

### 1. Velocity Score (Posting Speed)

Measures how fast the narrative spreads.

```python
def calculate_velocity_score(posts, narrative):
    timestamps = sorted([p.timestamp for p in posts])
    time_span = (timestamps[-1] - timestamps[0]).total_seconds()
    
    if time_span <= 0:
        return 1.0  # All posts at the same time = suspicious
    
    hours = time_span / 3600
    posts_per_hour = len(posts) / hours
    
    # Normalize: 10+ posts/hour = maximum score
    score = min(posts_per_hour / 10, 1.0)
    
    # Bonus for burst patterns
    burst_ratio = detect_bursts(timestamps)
    score = min(score + burst_ratio * 0.2, 1.0)
    
    return score
```

**Interpretation:**
| Posts/hour | Score | Interpretation |
|------------|-------|----------------|
| < 2 | 0.0 - 0.2 | Normal activity |
| 2 - 5 | 0.2 - 0.5 | Moderate activity |
| 5 - 10 | 0.5 - 1.0 | High activity |
| > 10 | 1.0 | Intensive campaign |

### 2. Coordination Density

Measures what proportion of authors show coordinated behavior.

```python
def calculate_coordination_score(narrative_id, groups, total_authors):
    # Find groups related to this narrative
    relevant_groups = [g for g in groups if narrative_id in g.narrative_ids]
    
    if not relevant_groups:
        return 0.0
    
    # Count coordinated authors
    coordinated_authors = set()
    total_group_score = 0.0
    
    for group in relevant_groups:
        coordinated_authors.update(group.author_ids)
        total_group_score += group.score * group.size
    
    # Ratio of coordinated authors
    coordination_ratio = len(coordinated_authors) / total_authors
    
    # Weight by average score
    avg_score = total_group_score / len(coordinated_authors)
    
    return coordination_ratio * 0.6 + avg_score * 0.4
```

**Interpretation:**
| Ratio | Score | Interpretation |
|-------|-------|----------------|
| < 10% | 0.0 - 0.1 | Minimal coordination |
| 10-30% | 0.1 - 0.3 | Low coordination |
| 30-50% | 0.3 - 0.5 | Moderate coordination |
| > 50% | 0.5 - 1.0 | High coordination |

### 3. Bot Score (Bot-like Patterns)

Detects automated behavior based on heuristics.

```python
def calculate_bot_score(posts):
    author_posts = group_by_author(posts)
    bot_indicators = []
    
    for author_id, posts in author_posts.items():
        indicators = 0.0
        
        # High posting frequency
        posts_per_hour = calculate_posting_rate(posts)
        if posts_per_hour > 20:
            indicators += 0.3
        
        # Repetitive content
        unique_ratio = len(set(p.text for p in posts)) / len(posts)
        if unique_ratio < 0.5:
            indicators += 0.3
        
        # Regular intervals (bot-like)
        interval_variance = calculate_interval_variance(posts)
        if interval_variance < 0.1:
            indicators += 0.2
        
        # High URL ratio
        url_ratio = sum(1 for p in posts if p.urls) / len(posts)
        if url_ratio > 0.8:
            indicators += 0.2
        
        bot_indicators.append(min(indicators, 1.0))
    
    return sum(bot_indicators) / len(bot_indicators)
```

**Bot Signals:**
| Signal | Weight | Description |
|--------|--------|-------------|
| High frequency | 0.3 | > 20 posts/hour |
| Repetitive content | 0.3 | < 50% unique posts |
| Regular intervals | 0.2 | Variance < 10% |
| High URL ratio | 0.2 | > 80% posts with URLs |

### 4. Foreign Domain Ratio

Measures the presence of domains from specific countries.

```python
def calculate_foreign_domain_score(posts, foreign_tlds=[".ru", ".cn", ".ir"]):
    all_domains = set()
    foreign_domains = set()
    
    for post in posts:
        for domain in post.domains:
            all_domains.add(domain)
            for tld in foreign_tlds:
                if domain.endswith(tld):
                    foreign_domains.add(domain)
                    break
    
    if not all_domains:
        return 0.0
    
    return len(foreign_domains) / len(all_domains)
```

**Configuration:**
```yaml
risk:
  foreign_tlds:
    - ".ru"
    - ".cn"
    - ".ir"
```

### 5. Toxicity Score

Detects potentially harmful content using keyword heuristics.

```python
def calculate_toxicity_score(posts):
    toxic_keywords = {
        "hate", "kill", "die", "attack", "destroy", "enemy",
        "threat", "dangerous", "evil", "corrupt", "conspiracy",
        "hoax", "fake", "propaganda", "lies", "traitor"
    }
    
    toxic_count = 0
    total_words = 0
    
    for post in posts:
        words = post.text.lower().split()
        total_words += len(words)
        
        for word in words:
            clean_word = ''.join(c for c in word if c.isalnum())
            if clean_word in toxic_keywords:
                toxic_count += 1
    
    # Normalize: > 5% toxic words = maximum score
    ratio = toxic_count / total_words
    return min(ratio / 0.05, 1.0)
```

**Note:** In production, use a toxicity model like Perspective API.

## Weight Configuration

```yaml
risk:
  weights:
    velocity: 0.25              # 25%
    coordination_density: 0.30  # 30%
    bot_score: 0.20            # 20%
    foreign_domain_ratio: 0.15 # 15%
    toxicity: 0.10             # 10%
```

### Weight Adjustment by Use Case

**Coordinated Campaign Detection:**
```yaml
risk:
  weights:
    velocity: 0.20
    coordination_density: 0.40  # Increased
    bot_score: 0.25
    foreign_domain_ratio: 0.10
    toxicity: 0.05
```

**Disinformation Detection:**
```yaml
risk:
  weights:
    velocity: 0.15
    coordination_density: 0.25
    bot_score: 0.15
    foreign_domain_ratio: 0.25  # Increased
    toxicity: 0.20              # Increased
```

## Risk Classification

```yaml
risk:
  thresholds:
    low: 0.3      # score < 0.3 → LOW
    medium: 0.6   # 0.3 ≤ score < 0.6 → MEDIUM
    high: 0.8     # score ≥ 0.6 → HIGH
```

### Risk Levels

| Level | Score | Recommended Action |
|-------|-------|-------------------|
| LOW | < 0.3 | Passive monitoring |
| MEDIUM | 0.3 - 0.6 | Manual review |
| HIGH | ≥ 0.6 | Priority investigation |

## Reason Generation

```python
def generate_reasons(components, weights):
    reasons = []
    threshold = 0.3  # Minimum to report
    
    if components.velocity >= threshold:
        contribution = weights.velocity * components.velocity
        reasons.append(
            f"High posting velocity ({components.velocity:.2f}) - "
            f"contributes {contribution:.2f} to risk"
        )
    
    # Similar for other components...
    
    if not reasons:
        reasons.append("No significant risk factors identified")
    
    return reasons
```

## Output

### NarrativeRisk Schema

```python
class NarrativeRisk(BaseModel):
    narrative_id: str
    risk_score: float           # 0.0 - 1.0
    risk_level: RiskLevel       # LOW, MEDIUM, HIGH
    components: RiskComponents  # Breakdown
    reasons: list[str]          # Explanations
```

### Output Example

```json
{
  "narrative_id": "narrative_0001",
  "risk_score": 0.72,
  "risk_level": "HIGH",
  "components": {
    "velocity": 0.85,
    "coordination_density": 0.65,
    "bot_score": 0.45,
    "foreign_domain_ratio": 0.30,
    "toxicity": 0.20
  },
  "reasons": [
    "High posting velocity (0.85) - contributes 0.21 to risk",
    "Coordinated behavior detected (0.65) - contributes 0.20 to risk",
    "Bot-like activity patterns (0.45) - contributes 0.09 to risk"
  ]
}
```

## Neo4j Storage

```cypher
MATCH (n:Narrative {id: $narrative_id})
SET n.risk_score = $risk_score,
    n.risk_level = $risk_level,
    n.risk_components = $risk_components,
    n.explanation = $explanation
RETURN n.id as id
```

## Analysis Queries

### High Risk Narratives

```cypher
MATCH (n:Narrative)
WHERE n.risk_level = 'HIGH'
RETURN n.id, n.risk_score, n.keywords, n.size
ORDER BY n.risk_score DESC
```

### Risk Distribution

```cypher
MATCH (n:Narrative)
RETURN n.risk_level, count(n) as count, avg(n.risk_score) as avg_score
ORDER BY avg_score DESC
```

### Top Narratives by Component

```cypher
// By velocity
MATCH (n:Narrative)
RETURN n.id, n.velocity, n.risk_score
ORDER BY n.velocity DESC
LIMIT 10
```

## Validation and Calibration

### Validation Metrics

1. **Precision**: How many HIGH narratives are actually problematic?
2. **Recall**: How many problematic narratives were detected?
3. **Calibration**: Do scores reflect actual probabilities?

### Calibration Process

1. Manually label a set of narratives
2. Calculate validation metrics
3. Adjust weights and thresholds
4. Re-evaluate

### Calibration Example

```python
# If there are many false positives in HIGH
risk:
  thresholds:
    high: 0.75  # Raise threshold (was 0.6)

# If there are many false negatives
risk:
  weights:
    coordination_density: 0.35  # Increase weight
```
