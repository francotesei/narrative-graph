"""Tests for graph module (mocked, no actual Neo4j required)."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from narrative_graph.ingestion.schemas import (
    NarrativeMetadata,
    NormalizedPost,
    Platform,
    PostEntities,
    ExtractedEntity,
)


@pytest.fixture
def mock_neo4j_connection():
    """Create a mock Neo4j connection."""
    mock_conn = MagicMock()
    mock_conn.verify_connectivity.return_value = True
    mock_conn.execute_read.return_value = []
    mock_conn.execute_write.return_value = []
    mock_conn.execute_batch_write.return_value = 10
    return mock_conn


@pytest.fixture
def sample_posts_for_graph():
    """Create sample posts for graph building."""
    return [
        NormalizedPost(
            id="post_001",
            timestamp=datetime(2024, 1, 15, 10, 0),
            platform=Platform.TWITTER,
            author_id="user_001",
            author_handle="@user1",
            text="Test post about policy",
            domains=["example.com"],
            hashtags=["policy"],
            narrative_id="narrative_001",
            cluster_similarity=0.9,
        ),
        NormalizedPost(
            id="post_002",
            timestamp=datetime(2024, 1, 15, 10, 5),
            platform=Platform.TWITTER,
            author_id="user_002",
            author_handle="@user2",
            text="Another test post",
            domains=["news.org"],
            hashtags=["policy", "news"],
            narrative_id="narrative_001",
            cluster_similarity=0.85,
        ),
    ]


@pytest.fixture
def sample_narratives_for_graph():
    """Create sample narratives for graph building."""
    return [
        NarrativeMetadata(
            id="narrative_001",
            size=10,
            keywords=["policy", "change"],
            top_domains=["example.com"],
            top_hashtags=["policy"],
            start_time=datetime(2024, 1, 15, 10, 0),
            end_time=datetime(2024, 1, 15, 12, 0),
            author_count=5,
        )
    ]


@pytest.fixture
def sample_entities_for_graph():
    """Create sample entities for graph building."""
    return [
        PostEntities(
            post_id="post_001",
            entities=[
                ExtractedEntity(name="John Doe", type="PERSON"),
                ExtractedEntity(name="Acme Corp", type="ORG"),
            ],
        ),
        PostEntities(
            post_id="post_002",
            entities=[
                ExtractedEntity(name="Jane Smith", type="PERSON"),
            ],
        ),
    ]


class TestGraphBuilder:
    """Test graph builder functionality."""

    def test_build_from_posts(
        self,
        mock_neo4j_connection,
        sample_posts_for_graph,
        sample_narratives_for_graph,
        sample_entities_for_graph,
    ):
        """Test building graph from posts."""
        from narrative_graph.graph.builder import GraphBuilder

        builder = GraphBuilder(connection=mock_neo4j_connection)

        stats = builder.build_from_posts(
            sample_posts_for_graph,
            sample_narratives_for_graph,
            sample_entities_for_graph,
        )

        # Verify that batch writes were called
        assert mock_neo4j_connection.execute_batch_write.called

        # Stats should have counts
        assert "authors" in stats
        assert "posts" in stats
        assert "narratives" in stats

    def test_create_authors(self, mock_neo4j_connection, sample_posts_for_graph):
        """Test author node creation."""
        from narrative_graph.graph.builder import GraphBuilder

        builder = GraphBuilder(connection=mock_neo4j_connection)
        count = builder._create_authors(sample_posts_for_graph, batch_size=100)

        # Should create 2 unique authors
        assert count == 2
        mock_neo4j_connection.execute_batch_write.assert_called()

    def test_create_relationships(
        self, mock_neo4j_connection, sample_posts_for_graph
    ):
        """Test relationship creation."""
        from narrative_graph.graph.builder import GraphBuilder

        builder = GraphBuilder(connection=mock_neo4j_connection)

        # Test POSTED relationships
        count = builder._create_posted_relationships(sample_posts_for_graph, batch_size=100)
        assert count == 2

        # Test BELONGS_TO relationships
        count = builder._create_belongs_to_relationships(sample_posts_for_graph, batch_size=100)
        assert count == 2  # Both posts have narrative_id


class TestGraphMetrics:
    """Test graph metrics calculation."""

    def test_calculate_degree_centrality(self, mock_neo4j_connection):
        """Test degree centrality calculation."""
        from narrative_graph.graph.metrics import GraphMetrics

        mock_neo4j_connection.execute_write.return_value = [{"updated": 10}]

        metrics = GraphMetrics(connection=mock_neo4j_connection)
        count = metrics.calculate_degree_centrality()

        assert count == 10
        mock_neo4j_connection.execute_write.assert_called()

    def test_get_graph_summary(self, mock_neo4j_connection):
        """Test graph summary retrieval."""
        from narrative_graph.graph.metrics import GraphMetrics

        mock_neo4j_connection.execute_read.return_value = [{"count": 100}]

        metrics = GraphMetrics(connection=mock_neo4j_connection)
        summary = metrics.get_graph_summary()

        assert isinstance(summary, dict)
        mock_neo4j_connection.execute_read.assert_called()


class TestGraphExporter:
    """Test graph export functionality."""

    def test_export_narrative_subgraph(self, mock_neo4j_connection):
        """Test subgraph export."""
        from narrative_graph.graph.export import GraphExporter

        mock_neo4j_connection.execute_read.return_value = [
            {
                "nodes": [
                    {"id": 1, "labels": ["Post"], "properties": {"id": "post_001"}},
                    {"id": 2, "labels": ["Author"], "properties": {"id": "user_001"}},
                ],
                "edges": [
                    {"source": 2, "target": 1, "type": "POSTED"},
                ],
            }
        ]

        exporter = GraphExporter(connection=mock_neo4j_connection)
        graph_data = exporter.export_narrative_subgraph("narrative_001")

        assert "nodes" in graph_data
        assert "edges" in graph_data
        assert "node_count" in graph_data
        assert "edge_count" in graph_data
