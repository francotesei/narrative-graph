"""Graph builder for Neo4j."""

from collections import defaultdict
from typing import Any

from narrative_graph.graph.connection import Neo4jConnection, get_neo4j_connection
from narrative_graph.graph import queries
from narrative_graph.ingestion.schemas import (
    NarrativeMetadata,
    NormalizedPost,
    PostEntities,
)
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


class GraphBuilder:
    """Builder for constructing the narrative graph in Neo4j."""

    def __init__(self, connection: Neo4jConnection | None = None):
        """Initialize graph builder.

        Args:
            connection: Neo4j connection instance
        """
        self.conn = connection or get_neo4j_connection()

    def build_from_posts(
        self,
        posts: list[NormalizedPost],
        narratives: list[NarrativeMetadata],
        entities: list[PostEntities] | None = None,
        batch_size: int = 500,
    ) -> dict[str, int]:
        """Build complete graph from posts and narratives.

        Args:
            posts: List of normalized posts
            narratives: List of narrative metadata
            entities: Optional list of extracted entities per post
            batch_size: Batch size for Neo4j operations

        Returns:
            Dictionary with counts of created nodes/relationships
        """
        logger.info(
            "graph_build_started",
            post_count=len(posts),
            narrative_count=len(narratives),
        )

        stats: dict[str, int] = {}

        # Create authors
        stats["authors"] = self._create_authors(posts, batch_size)

        # Create posts
        stats["posts"] = self._create_posts(posts, batch_size)

        # Create narratives
        stats["narratives"] = self._create_narratives(narratives, batch_size)

        # Create domains
        stats["domains"] = self._create_domains(posts, batch_size)

        # Create hashtags
        stats["hashtags"] = self._create_hashtags(posts, batch_size)

        # Create entities if provided
        if entities:
            stats["entities"] = self._create_entities(entities, batch_size)

        # Create relationships
        stats["rel_posted"] = self._create_posted_relationships(posts, batch_size)
        stats["rel_belongs_to"] = self._create_belongs_to_relationships(posts, batch_size)
        stats["rel_links_to"] = self._create_links_to_relationships(posts, batch_size)
        stats["rel_tagged_with"] = self._create_tagged_with_relationships(posts, batch_size)

        if entities:
            stats["rel_mentions"] = self._create_mentions_relationships(entities, batch_size)

        logger.info("graph_build_completed", stats=stats)

        return stats

    def _create_authors(self, posts: list[NormalizedPost], batch_size: int) -> int:
        """Create Author nodes."""
        # Aggregate author data
        authors: dict[str, dict[str, Any]] = {}

        for post in posts:
            if post.author_id not in authors:
                authors[post.author_id] = {
                    "id": post.author_id,
                    "handle": post.author_handle,
                    "platform": post.platform.value,
                }

        # Create in batches
        author_list = list(authors.values())
        created = 0

        for i in range(0, len(author_list), batch_size):
            batch = author_list[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_AUTHORS_BATCH, batch)

        logger.debug("authors_created", count=len(authors))
        return len(authors)

    def _create_posts(self, posts: list[NormalizedPost], batch_size: int) -> int:
        """Create Post nodes."""
        post_data = [
            {
                "id": post.id,
                "text": post.text[:5000],  # Truncate very long texts
                "timestamp": post.timestamp.isoformat(),
                "platform": post.platform.value,
                "lang": post.lang,
                "author_id": post.author_id,
            }
            for post in posts
        ]

        created = 0
        for i in range(0, len(post_data), batch_size):
            batch = post_data[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_POSTS_BATCH, batch)

        logger.debug("posts_created", count=len(posts))
        return len(posts)

    def _create_narratives(
        self, narratives: list[NarrativeMetadata], batch_size: int
    ) -> int:
        """Create Narrative nodes."""
        narrative_data = [
            {
                "id": n.id,
                "size": n.size,
                "keywords": n.keywords,
                "top_domains": n.top_domains,
                "top_hashtags": n.top_hashtags,
                "start_time": n.start_time.isoformat() if n.start_time else None,
                "end_time": n.end_time.isoformat() if n.end_time else None,
            }
            for n in narratives
        ]

        created = 0
        for i in range(0, len(narrative_data), batch_size):
            batch = narrative_data[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_NARRATIVES_BATCH, batch)

        logger.debug("narratives_created", count=len(narratives))
        return len(narratives)

    def _create_domains(self, posts: list[NormalizedPost], batch_size: int) -> int:
        """Create Domain nodes."""
        domain_counts: dict[str, int] = defaultdict(int)

        for post in posts:
            for domain in post.domains:
                domain_counts[domain] += 1

        domain_data = [{"name": name, "count": count} for name, count in domain_counts.items()]

        created = 0
        for i in range(0, len(domain_data), batch_size):
            batch = domain_data[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_DOMAINS_BATCH, batch)

        logger.debug("domains_created", count=len(domain_counts))
        return len(domain_counts)

    def _create_hashtags(self, posts: list[NormalizedPost], batch_size: int) -> int:
        """Create Hashtag nodes."""
        hashtag_counts: dict[str, int] = defaultdict(int)

        for post in posts:
            for hashtag in post.hashtags:
                hashtag_counts[hashtag] += 1

        hashtag_data = [{"tag": tag, "count": count} for tag, count in hashtag_counts.items()]

        created = 0
        for i in range(0, len(hashtag_data), batch_size):
            batch = hashtag_data[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_HASHTAGS_BATCH, batch)

        logger.debug("hashtags_created", count=len(hashtag_counts))
        return len(hashtag_counts)

    def _create_entities(self, entities: list[PostEntities], batch_size: int) -> int:
        """Create Entity nodes."""
        entity_counts: dict[tuple[str, str], int] = defaultdict(int)

        for post_entities in entities:
            for entity in post_entities.entities:
                key = (entity.name, entity.type)
                entity_counts[key] += 1

        entity_data = [
            {"name": name, "type": etype, "count": count}
            for (name, etype), count in entity_counts.items()
        ]

        created = 0
        for i in range(0, len(entity_data), batch_size):
            batch = entity_data[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_ENTITIES_BATCH, batch)

        logger.debug("entities_created", count=len(entity_counts))
        return len(entity_counts)

    def _create_posted_relationships(
        self, posts: list[NormalizedPost], batch_size: int
    ) -> int:
        """Create POSTED relationships between Authors and Posts."""
        rel_data = [
            {
                "author_id": post.author_id,
                "post_id": post.id,
                "timestamp": post.timestamp.isoformat(),
            }
            for post in posts
        ]

        created = 0
        for i in range(0, len(rel_data), batch_size):
            batch = rel_data[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_AUTHOR_POSTED_BATCH, batch)

        logger.debug("posted_relationships_created", count=len(posts))
        return len(posts)

    def _create_belongs_to_relationships(
        self, posts: list[NormalizedPost], batch_size: int
    ) -> int:
        """Create BELONGS_TO relationships between Posts and Narratives."""
        rel_data = [
            {
                "post_id": post.id,
                "narrative_id": post.narrative_id,
                "similarity_score": post.cluster_similarity or 0.0,
            }
            for post in posts
            if post.narrative_id and post.narrative_id != "noise"
        ]

        created = 0
        for i in range(0, len(rel_data), batch_size):
            batch = rel_data[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_POST_BELONGS_TO_BATCH, batch)

        logger.debug("belongs_to_relationships_created", count=len(rel_data))
        return len(rel_data)

    def _create_links_to_relationships(
        self, posts: list[NormalizedPost], batch_size: int
    ) -> int:
        """Create LINKS_TO relationships between Posts and Domains."""
        rel_data = []

        for post in posts:
            for domain in post.domains:
                rel_data.append({"post_id": post.id, "domain": domain})

        created = 0
        for i in range(0, len(rel_data), batch_size):
            batch = rel_data[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_POST_LINKS_TO_BATCH, batch)

        logger.debug("links_to_relationships_created", count=len(rel_data))
        return len(rel_data)

    def _create_tagged_with_relationships(
        self, posts: list[NormalizedPost], batch_size: int
    ) -> int:
        """Create TAGGED_WITH relationships between Posts and Hashtags."""
        rel_data = []

        for post in posts:
            for hashtag in post.hashtags:
                rel_data.append({"post_id": post.id, "hashtag": hashtag})

        created = 0
        for i in range(0, len(rel_data), batch_size):
            batch = rel_data[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_POST_TAGGED_WITH_BATCH, batch)

        logger.debug("tagged_with_relationships_created", count=len(rel_data))
        return len(rel_data)

    def _create_mentions_relationships(
        self, entities: list[PostEntities], batch_size: int
    ) -> int:
        """Create MENTIONS relationships between Posts and Entities."""
        rel_data = []

        for post_entities in entities:
            for entity in post_entities.entities:
                rel_data.append(
                    {
                        "post_id": post_entities.post_id,
                        "entity_name": entity.name,
                        "entity_type": entity.type,
                    }
                )

        created = 0
        for i in range(0, len(rel_data), batch_size):
            batch = rel_data[i : i + batch_size]
            created += self.conn.execute_batch_write(queries.CREATE_POST_MENTIONS_BATCH, batch)

        logger.debug("mentions_relationships_created", count=len(rel_data))
        return len(rel_data)
