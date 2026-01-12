"""Coordination detection using heuristics and graph patterns."""

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from narrative_graph.config import get_settings
from narrative_graph.graph.connection import Neo4jConnection, get_neo4j_connection
from narrative_graph.graph import queries
from narrative_graph.ingestion.schemas import (
    CoordinatedGroup,
    CoordinatedPair,
    CoordinationEvidence,
    NormalizedPost,
)
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


class CoordinationDetector:
    """Detect coordinated behavior among authors."""

    def __init__(
        self,
        connection: Neo4jConnection | None = None,
        time_window_minutes: int | None = None,
        similarity_threshold: float | None = None,
        min_group_size: int | None = None,
    ):
        """Initialize coordination detector.

        Args:
            connection: Neo4j connection instance
            time_window_minutes: Time window for temporal proximity
            similarity_threshold: Minimum similarity for coordination
            min_group_size: Minimum size for coordination groups
        """
        settings = get_settings()
        self.conn = connection or get_neo4j_connection()
        self.time_window = timedelta(
            minutes=time_window_minutes or settings.coordination.time_window_minutes
        )
        self.similarity_threshold = (
            similarity_threshold or settings.coordination.similarity_threshold
        )
        self.min_group_size = min_group_size or settings.coordination.min_group_size

        # Weights for coordination score
        self.text_weight = settings.coordination.text_similarity_weight
        self.domain_weight = settings.coordination.shared_domain_weight
        self.hashtag_weight = settings.coordination.shared_hashtag_weight

    def detect_from_posts(
        self,
        posts: list[NormalizedPost],
        embeddings: np.ndarray | None = None,
    ) -> tuple[list[CoordinatedPair], list[CoordinatedGroup]]:
        """Detect coordination from posts.

        Args:
            posts: List of normalized posts
            embeddings: Optional pre-computed embeddings

        Returns:
            Tuple of (coordinated pairs, coordinated groups)
        """
        logger.info("coordination_detection_started", post_count=len(posts))

        # Group posts by narrative
        narrative_posts: dict[str, list[tuple[NormalizedPost, int]]] = defaultdict(list)
        for idx, post in enumerate(posts):
            if post.narrative_id and post.narrative_id != "noise":
                narrative_posts[post.narrative_id].append((post, idx))

        all_pairs: list[CoordinatedPair] = []

        # Detect coordination within each narrative
        for narrative_id, post_data in narrative_posts.items():
            if len(post_data) < 2:
                continue

            pairs = self._detect_pairs_in_narrative(
                post_data, embeddings, narrative_id
            )
            all_pairs.extend(pairs)

        # Build coordination groups from pairs
        groups = self._build_groups(all_pairs)

        # Store coordination relationships in Neo4j
        self._store_coordination(all_pairs)

        logger.info(
            "coordination_detection_completed",
            pairs=len(all_pairs),
            groups=len(groups),
        )

        return all_pairs, groups

    def _detect_pairs_in_narrative(
        self,
        post_data: list[tuple[NormalizedPost, int]],
        embeddings: np.ndarray | None,
        narrative_id: str,
    ) -> list[CoordinatedPair]:
        """Detect coordinated pairs within a narrative.

        Args:
            post_data: List of (post, index) tuples
            embeddings: Optional embeddings array
            narrative_id: Narrative identifier

        Returns:
            List of coordinated pairs
        """
        pairs: list[CoordinatedPair] = []
        posts = [p for p, _ in post_data]
        indices = [i for _, i in post_data]

        # Group posts by author
        author_posts: dict[str, list[tuple[NormalizedPost, int]]] = defaultdict(list)
        for post, idx in post_data:
            author_posts[post.author_id].append((post, idx))

        # Compare posts from different authors
        author_ids = list(author_posts.keys())

        for i, author1 in enumerate(author_ids):
            for author2 in author_ids[i + 1 :]:
                score, evidence = self._calculate_pair_score(
                    author_posts[author1],
                    author_posts[author2],
                    embeddings,
                )

                if score >= self.similarity_threshold:
                    pairs.append(
                        CoordinatedPair(
                            author1_id=author1,
                            author2_id=author2,
                            score=score,
                            evidence=evidence,
                            narrative_id=narrative_id,
                        )
                    )

        return pairs

    def _calculate_pair_score(
        self,
        posts1: list[tuple[NormalizedPost, int]],
        posts2: list[tuple[NormalizedPost, int]],
        embeddings: np.ndarray | None,
    ) -> tuple[float, CoordinationEvidence]:
        """Calculate coordination score between two authors.

        Args:
            posts1: Posts from first author
            posts2: Posts from second author
            embeddings: Optional embeddings array

        Returns:
            Tuple of (score, evidence)
        """
        evidence = CoordinationEvidence()
        scores: list[float] = []

        # Find temporally proximate post pairs
        for post1, idx1 in posts1:
            for post2, idx2 in posts2:
                time_diff = abs((post1.timestamp - post2.timestamp).total_seconds())

                if time_diff <= self.time_window.total_seconds():
                    evidence.post_ids.extend([post1.id, post2.id])
                    evidence.time_delta_seconds = time_diff

                    # Text similarity
                    text_sim = 0.0
                    if embeddings is not None:
                        emb1 = embeddings[idx1].reshape(1, -1)
                        emb2 = embeddings[idx2].reshape(1, -1)
                        text_sim = float(cosine_similarity(emb1, emb2)[0, 0])

                    # Shared domains
                    shared_domains = set(post1.domains) & set(post2.domains)
                    domain_sim = len(shared_domains) / max(
                        len(set(post1.domains) | set(post2.domains)), 1
                    )
                    evidence.shared_domains.extend(shared_domains)

                    # Shared hashtags
                    shared_hashtags = set(post1.hashtags) & set(post2.hashtags)
                    hashtag_sim = len(shared_hashtags) / max(
                        len(set(post1.hashtags) | set(post2.hashtags)), 1
                    )
                    evidence.shared_hashtags.extend(shared_hashtags)

                    # Combined score
                    pair_score = (
                        self.text_weight * text_sim
                        + self.domain_weight * domain_sim
                        + self.hashtag_weight * hashtag_sim
                    )
                    scores.append(pair_score)

        if not scores:
            return 0.0, evidence

        # Deduplicate evidence
        evidence.post_ids = list(set(evidence.post_ids))
        evidence.shared_domains = list(set(evidence.shared_domains))
        evidence.shared_hashtags = list(set(evidence.shared_hashtags))
        evidence.text_similarity = max(scores) if scores else None

        return max(scores), evidence

    def _build_groups(self, pairs: list[CoordinatedPair]) -> list[CoordinatedGroup]:
        """Build coordination groups from pairs using connected components.

        Args:
            pairs: List of coordinated pairs

        Returns:
            List of coordination groups
        """
        if not pairs:
            return []

        # Build adjacency list
        graph: dict[str, set[str]] = defaultdict(set)
        pair_scores: dict[tuple[str, str], CoordinatedPair] = {}

        for pair in pairs:
            graph[pair.author1_id].add(pair.author2_id)
            graph[pair.author2_id].add(pair.author1_id)
            key = tuple(sorted([pair.author1_id, pair.author2_id]))
            pair_scores[key] = pair  # type: ignore

        # Find connected components using BFS
        visited: set[str] = set()
        groups: list[CoordinatedGroup] = []
        group_id = 0

        for start_author in graph:
            if start_author in visited:
                continue

            # BFS to find component
            component: list[str] = []
            queue = [start_author]

            while queue:
                author = queue.pop(0)
                if author in visited:
                    continue

                visited.add(author)
                component.append(author)

                for neighbor in graph[author]:
                    if neighbor not in visited:
                        queue.append(neighbor)

            if len(component) >= self.min_group_size:
                # Calculate group score (average of pair scores)
                group_scores = []
                narrative_ids = set()

                for i, a1 in enumerate(component):
                    for a2 in component[i + 1 :]:
                        key = tuple(sorted([a1, a2]))
                        if key in pair_scores:
                            group_scores.append(pair_scores[key].score)  # type: ignore
                            if pair_scores[key].narrative_id:  # type: ignore
                                narrative_ids.add(pair_scores[key].narrative_id)  # type: ignore

                avg_score = sum(group_scores) / len(group_scores) if group_scores else 0

                groups.append(
                    CoordinatedGroup(
                        id=f"coord_group_{group_id:04d}",
                        author_ids=component,
                        score=avg_score,
                        evidence_summary=f"Group of {len(component)} authors with avg coordination score {avg_score:.2f}",
                        narrative_ids=list(narrative_ids),
                        size=len(component),
                    )
                )
                group_id += 1

        # Sort by score
        groups.sort(key=lambda g: g.score, reverse=True)

        return groups

    def _store_coordination(self, pairs: list[CoordinatedPair]) -> int:
        """Store coordination relationships in Neo4j.

        Args:
            pairs: List of coordinated pairs

        Returns:
            Number of relationships created
        """
        if not pairs:
            return 0

        batch_data = [
            {
                "author1_id": pair.author1_id,
                "author2_id": pair.author2_id,
                "score": pair.score,
                "evidence": pair.evidence.model_dump_json(),
                "narrative_id": pair.narrative_id,
            }
            for pair in pairs
        ]

        created = self.conn.execute_batch_write(
            queries.CREATE_COORDINATED_WITH_BATCH, batch_data
        )

        # Update author coordination scores
        update_query = """
        MATCH (a:Author)-[r:COORDINATED_WITH]-()
        WITH a, avg(r.score) as avg_score, count(r) as coord_count
        SET a.coordination_score = avg_score,
            a.coordination_count = coord_count
        RETURN count(a) as updated
        """
        self.conn.execute_write(update_query)

        logger.debug("coordination_stored", relationships=created)
        return created


def detect_coordination(
    posts: list[NormalizedPost],
    embeddings: np.ndarray | None = None,
    connection: Neo4jConnection | None = None,
) -> tuple[list[CoordinatedPair], list[CoordinatedGroup]]:
    """Convenience function to detect coordination.

    Args:
        posts: List of normalized posts
        embeddings: Optional pre-computed embeddings
        connection: Optional Neo4j connection

    Returns:
        Tuple of (coordinated pairs, coordinated groups)
    """
    detector = CoordinationDetector(connection=connection)
    return detector.detect_from_posts(posts, embeddings)
