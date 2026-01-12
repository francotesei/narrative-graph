"""Predefined Cypher queries for common operations."""

# =============================================================================
# Node Creation Queries
# =============================================================================

CREATE_AUTHORS_BATCH = """
UNWIND $batch AS item
MERGE (a:Author {id: item.id})
SET a.handle = item.handle,
    a.platform = item.platform,
    a.post_count = coalesce(a.post_count, 0) + 1,
    a.updated_at = datetime()
RETURN count(a) as created
"""

CREATE_POSTS_BATCH = """
UNWIND $batch AS item
MERGE (p:Post {id: item.id})
SET p.text = item.text,
    p.timestamp = datetime(item.timestamp),
    p.platform = item.platform,
    p.lang = item.lang,
    p.author_id = item.author_id,
    p.created_at = datetime()
RETURN count(p) as created
"""

CREATE_NARRATIVES_BATCH = """
UNWIND $batch AS item
MERGE (n:Narrative {id: item.id})
SET n.size = item.size,
    n.keywords = item.keywords,
    n.top_domains = item.top_domains,
    n.top_hashtags = item.top_hashtags,
    n.start_time = datetime(item.start_time),
    n.end_time = datetime(item.end_time),
    n.created_at = datetime()
RETURN count(n) as created
"""

CREATE_DOMAINS_BATCH = """
UNWIND $batch AS item
MERGE (d:Domain {name: item.name})
SET d.post_count = coalesce(d.post_count, 0) + item.count
RETURN count(d) as created
"""

CREATE_HASHTAGS_BATCH = """
UNWIND $batch AS item
MERGE (h:Hashtag {tag: item.tag})
SET h.post_count = coalesce(h.post_count, 0) + item.count
RETURN count(h) as created
"""

CREATE_ENTITIES_BATCH = """
UNWIND $batch AS item
MERGE (e:Entity {name: item.name, type: item.type})
SET e.mention_count = coalesce(e.mention_count, 0) + item.count
RETURN count(e) as created
"""

# =============================================================================
# Relationship Creation Queries
# =============================================================================

CREATE_AUTHOR_POSTED_BATCH = """
UNWIND $batch AS item
MATCH (a:Author {id: item.author_id})
MATCH (p:Post {id: item.post_id})
MERGE (a)-[r:POSTED]->(p)
SET r.timestamp = datetime(item.timestamp)
RETURN count(r) as created
"""

CREATE_POST_BELONGS_TO_BATCH = """
UNWIND $batch AS item
MATCH (p:Post {id: item.post_id})
MATCH (n:Narrative {id: item.narrative_id})
MERGE (p)-[r:BELONGS_TO]->(n)
SET r.similarity_score = item.similarity_score
RETURN count(r) as created
"""

CREATE_POST_LINKS_TO_BATCH = """
UNWIND $batch AS item
MATCH (p:Post {id: item.post_id})
MATCH (d:Domain {name: item.domain})
MERGE (p)-[r:LINKS_TO]->(d)
RETURN count(r) as created
"""

CREATE_POST_TAGGED_WITH_BATCH = """
UNWIND $batch AS item
MATCH (p:Post {id: item.post_id})
MATCH (h:Hashtag {tag: item.hashtag})
MERGE (p)-[r:TAGGED_WITH]->(h)
RETURN count(r) as created
"""

CREATE_POST_MENTIONS_BATCH = """
UNWIND $batch AS item
MATCH (p:Post {id: item.post_id})
MATCH (e:Entity {name: item.entity_name, type: item.entity_type})
MERGE (p)-[r:MENTIONS]->(e)
RETURN count(r) as created
"""

CREATE_COORDINATED_WITH_BATCH = """
UNWIND $batch AS item
MATCH (a1:Author {id: item.author1_id})
MATCH (a2:Author {id: item.author2_id})
WHERE a1.id < a2.id
MERGE (a1)-[r:COORDINATED_WITH]-(a2)
SET r.score = item.score,
    r.evidence = item.evidence,
    r.narrative_id = item.narrative_id
RETURN count(r) as created
"""

# =============================================================================
# Read Queries
# =============================================================================

GET_ALL_NARRATIVES = """
MATCH (n:Narrative)
OPTIONAL MATCH (p:Post)-[:BELONGS_TO]->(n)
WITH n, count(p) as post_count
RETURN n {
    .id, .size, .keywords, .top_domains, .top_hashtags,
    .risk_score, .risk_level, .explanation,
    .start_time, .end_time,
    post_count: post_count
}
ORDER BY n.risk_score DESC
"""

GET_NARRATIVE_BY_ID = """
MATCH (n:Narrative {id: $narrative_id})
OPTIONAL MATCH (p:Post)-[:BELONGS_TO]->(n)
OPTIONAL MATCH (p)-[:TAGGED_WITH]->(h:Hashtag)
OPTIONAL MATCH (p)-[:LINKS_TO]->(d:Domain)
OPTIONAL MATCH (a:Author)-[:POSTED]->(p)
WITH n, 
     collect(DISTINCT p) as posts,
     collect(DISTINCT h.tag) as hashtags,
     collect(DISTINCT d.name) as domains,
     collect(DISTINCT a) as authors
RETURN n {
    .id, .size, .keywords, .top_domains, .top_hashtags,
    .risk_score, .risk_level, .explanation, .risk_components,
    .start_time, .end_time,
    post_count: size(posts),
    hashtags: hashtags,
    domains: domains,
    author_count: size(authors)
}
"""

GET_NARRATIVE_POSTS = """
MATCH (p:Post)-[:BELONGS_TO]->(n:Narrative {id: $narrative_id})
MATCH (a:Author)-[:POSTED]->(p)
RETURN p {
    .id, .text, .timestamp, .platform, .lang,
    author: a {.id, .handle, .platform}
}
ORDER BY p.timestamp DESC
LIMIT $limit
"""

GET_NARRATIVE_TOP_AMPLIFIERS = """
MATCH (a:Author)-[:POSTED]->(p:Post)-[:BELONGS_TO]->(n:Narrative {id: $narrative_id})
WITH a, count(p) as post_count
RETURN a {
    .id, .handle, .platform, .coordination_score,
    post_count: post_count
}
ORDER BY post_count DESC
LIMIT $limit
"""

GET_COORDINATED_GROUPS = """
MATCH (a1:Author)-[r:COORDINATED_WITH]-(a2:Author)
WHERE r.score >= $min_score
WITH a1, collect({author: a2, score: r.score, evidence: r.evidence}) as connections
WHERE size(connections) >= $min_group_size - 1
RETURN a1 {
    .id, .handle, .platform,
    connections: connections
}
ORDER BY size(connections) DESC
"""

GET_COORDINATION_BY_NARRATIVE = """
MATCH (a1:Author)-[r:COORDINATED_WITH]-(a2:Author)
WHERE r.narrative_id = $narrative_id
RETURN a1.id as author1, a2.id as author2, r.score as score, r.evidence as evidence
"""

# =============================================================================
# Metrics Queries
# =============================================================================

CALCULATE_DEGREE_CENTRALITY = """
MATCH (a:Author)-[:POSTED]->(p:Post)
WITH a, count(p) as degree
SET a.degree_centrality = degree
RETURN count(a) as updated
"""

CALCULATE_NARRATIVE_VELOCITY = """
MATCH (p:Post)-[:BELONGS_TO]->(n:Narrative)
WITH n, 
     min(p.timestamp) as start_time,
     max(p.timestamp) as end_time,
     count(p) as post_count
WITH n, post_count,
     duration.between(start_time, end_time).hours as hours
SET n.velocity = CASE WHEN hours > 0 THEN toFloat(post_count) / hours ELSE toFloat(post_count) END
RETURN count(n) as updated
"""

UPDATE_NARRATIVE_RISK = """
MATCH (n:Narrative {id: $narrative_id})
SET n.risk_score = $risk_score,
    n.risk_level = $risk_level,
    n.risk_components = $risk_components,
    n.explanation = $explanation
RETURN n.id as id
"""

# =============================================================================
# Graph Export Queries
# =============================================================================

EXPORT_SUBGRAPH_FOR_NARRATIVE = """
MATCH (p:Post)-[:BELONGS_TO]->(n:Narrative {id: $narrative_id})
OPTIONAL MATCH (a:Author)-[:POSTED]->(p)
OPTIONAL MATCH (p)-[:LINKS_TO]->(d:Domain)
OPTIONAL MATCH (p)-[:TAGGED_WITH]->(h:Hashtag)
OPTIONAL MATCH (p)-[:MENTIONS]->(e:Entity)
WITH collect(DISTINCT p) + collect(DISTINCT a) + collect(DISTINCT d) + 
     collect(DISTINCT h) + collect(DISTINCT e) + collect(DISTINCT n) as nodes
UNWIND nodes as node
WITH collect(DISTINCT node) as all_nodes
MATCH (n1)-[r]->(n2)
WHERE n1 IN all_nodes AND n2 IN all_nodes
RETURN collect(DISTINCT n1) as nodes, collect(DISTINCT {source: id(n1), target: id(n2), type: type(r)}) as relationships
"""
