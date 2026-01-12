"""Graph metrics calculation in Neo4j."""

from typing import Any

from narrative_graph.graph.connection import Neo4jConnection, get_neo4j_connection
from narrative_graph.graph import queries
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


class GraphMetrics:
    """Calculate graph metrics using Neo4j."""

    def __init__(self, connection: Neo4jConnection | None = None):
        """Initialize metrics calculator.

        Args:
            connection: Neo4j connection instance
        """
        self.conn = connection or get_neo4j_connection()

    def calculate_degree_centrality(self) -> int:
        """Calculate and store degree centrality for authors.

        Returns:
            Number of authors updated
        """
        result = self.conn.execute_write(queries.CALCULATE_DEGREE_CENTRALITY)
        count = result[0]["updated"] if result else 0
        logger.info("degree_centrality_calculated", updated=count)
        return count

    def calculate_narrative_velocity(self) -> int:
        """Calculate posting velocity for narratives.

        Returns:
            Number of narratives updated
        """
        result = self.conn.execute_write(queries.CALCULATE_NARRATIVE_VELOCITY)
        count = result[0]["updated"] if result else 0
        logger.info("narrative_velocity_calculated", updated=count)
        return count

    def get_top_amplifiers(
        self, narrative_id: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Get top amplifiers (most active authors) for a narrative.

        Args:
            narrative_id: Narrative identifier
            limit: Maximum number of results

        Returns:
            List of author records with post counts
        """
        result = self.conn.execute_read(
            queries.GET_NARRATIVE_TOP_AMPLIFIERS,
            {"narrative_id": narrative_id, "limit": limit},
        )
        return result

    def get_narrative_stats(self, narrative_id: str) -> dict[str, Any]:
        """Get comprehensive stats for a narrative.

        Args:
            narrative_id: Narrative identifier

        Returns:
            Dictionary with narrative statistics
        """
        # Get basic narrative info
        narrative_result = self.conn.execute_read(
            queries.GET_NARRATIVE_BY_ID,
            {"narrative_id": narrative_id},
        )

        if not narrative_result:
            return {}

        narrative = narrative_result[0]["n"]

        # Get top amplifiers
        amplifiers = self.get_top_amplifiers(narrative_id, limit=5)

        return {
            **narrative,
            "top_amplifiers": amplifiers,
        }

    def calculate_betweenness_centrality_approximate(self, sample_size: int = 100) -> int:
        """Calculate approximate betweenness centrality using sampling.

        This is a simplified version that doesn't require GDS plugin.

        Args:
            sample_size: Number of random paths to sample

        Returns:
            Number of nodes updated
        """
        # Simple degree-based approximation
        query = """
        MATCH (a:Author)-[:POSTED]->(p:Post)
        WITH a, count(p) as posts
        MATCH (a)-[:POSTED]->(:Post)-[:BELONGS_TO]->(n:Narrative)
        WITH a, posts, count(DISTINCT n) as narratives
        SET a.betweenness_approx = posts * narratives
        RETURN count(a) as updated
        """

        result = self.conn.execute_write(query)
        count = result[0]["updated"] if result else 0
        logger.info("betweenness_centrality_approximated", updated=count)
        return count

    def calculate_all_metrics(self) -> dict[str, int]:
        """Calculate all graph metrics.

        Returns:
            Dictionary with counts of updated nodes
        """
        logger.info("calculating_all_metrics")

        stats = {
            "degree_centrality": self.calculate_degree_centrality(),
            "narrative_velocity": self.calculate_narrative_velocity(),
            "betweenness_approx": self.calculate_betweenness_centrality_approximate(),
        }

        logger.info("all_metrics_calculated", stats=stats)
        return stats

    def get_graph_summary(self) -> dict[str, Any]:
        """Get summary statistics for the entire graph.

        Returns:
            Dictionary with graph statistics
        """
        summary: dict[str, Any] = {}

        # Node counts
        for label in ["Author", "Post", "Narrative", "Domain", "Hashtag", "Entity"]:
            query = f"MATCH (n:{label}) RETURN count(n) as count"
            result = self.conn.execute_read(query)
            summary[f"{label.lower()}_count"] = result[0]["count"] if result else 0

        # Relationship counts
        for rel_type in ["POSTED", "BELONGS_TO", "LINKS_TO", "TAGGED_WITH", "MENTIONS"]:
            query = f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
            result = self.conn.execute_read(query)
            summary[f"rel_{rel_type.lower()}_count"] = result[0]["count"] if result else 0

        # Top narratives by size
        query = """
        MATCH (n:Narrative)
        OPTIONAL MATCH (p:Post)-[:BELONGS_TO]->(n)
        WITH n, count(p) as post_count
        RETURN n.id as id, n.size as size, post_count
        ORDER BY post_count DESC
        LIMIT 5
        """
        result = self.conn.execute_read(query)
        summary["top_narratives"] = result

        return summary
