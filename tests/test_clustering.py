"""Tests for narrative clustering module."""

import numpy as np
import pytest

from narrative_graph.narratives.clustering import (
    cluster_hdbscan,
    cluster_kmeans,
    assign_narratives,
)
from narrative_graph.narratives.keywords import (
    extract_tfidf_keywords,
    extract_frequency_keywords,
)
from narrative_graph.ingestion.schemas import NormalizedPost, Platform
from datetime import datetime


@pytest.fixture
def sample_embeddings():
    """Generate sample embeddings for testing."""
    np.random.seed(42)

    # Create 3 clusters
    cluster1 = np.random.randn(10, 384) + np.array([1, 0, 0] + [0] * 381)
    cluster2 = np.random.randn(10, 384) + np.array([0, 1, 0] + [0] * 381)
    cluster3 = np.random.randn(10, 384) + np.array([0, 0, 1] + [0] * 381)

    return np.vstack([cluster1, cluster2, cluster3])


@pytest.fixture
def sample_normalized_posts():
    """Create sample normalized posts."""
    posts = []
    for i in range(30):
        cluster = i // 10
        posts.append(
            NormalizedPost(
                id=f"post_{i:03d}",
                timestamp=datetime(2024, 1, 15, 10 + cluster, i % 60),
                platform=Platform.TWITTER,
                author_id=f"user_{i % 5:03d}",
                text=f"Test post {i} about topic {cluster}",
                hashtags=[f"topic{cluster}"],
                domains=[f"domain{cluster}.com"],
            )
        )
    return posts


class TestClustering:
    """Test clustering algorithms."""

    def test_cluster_hdbscan(self, sample_embeddings):
        """Test HDBSCAN clustering."""
        labels, probabilities = cluster_hdbscan(
            sample_embeddings,
            min_cluster_size=3,
            min_samples=2,
        )

        assert len(labels) == len(sample_embeddings)
        assert len(probabilities) == len(sample_embeddings)

        # Should find some clusters (not all noise)
        unique_labels = set(labels)
        assert len(unique_labels) > 1 or -1 not in unique_labels

    def test_cluster_kmeans(self, sample_embeddings):
        """Test KMeans clustering."""
        labels, similarities = cluster_kmeans(
            sample_embeddings,
            n_clusters=3,
            random_state=42,
        )

        assert len(labels) == len(sample_embeddings)
        assert len(similarities) == len(sample_embeddings)

        # Should find exactly 3 clusters
        unique_labels = set(labels)
        assert len(unique_labels) == 3

        # Similarities should be between 0 and 1
        assert all(0 <= s <= 1 for s in similarities)

    def test_cluster_kmeans_deterministic(self, sample_embeddings):
        """Test that KMeans is deterministic with same seed."""
        labels1, _ = cluster_kmeans(sample_embeddings, n_clusters=3, random_state=42)
        labels2, _ = cluster_kmeans(sample_embeddings, n_clusters=3, random_state=42)

        assert np.array_equal(labels1, labels2)


class TestNarrativeAssignment:
    """Test narrative assignment."""

    def test_assign_narratives(self, sample_normalized_posts):
        """Test narrative assignment from labels."""
        # Simulate clustering results
        labels = np.array([i // 10 for i in range(30)])
        similarities = np.random.rand(30)

        posts, narratives = assign_narratives(
            sample_normalized_posts,
            labels,
            similarities,
        )

        assert len(posts) == 30
        assert len(narratives) == 3  # 3 clusters

        # All posts should have narrative_id
        assert all(p.narrative_id is not None for p in posts)

        # Narratives should have correct sizes
        sizes = [n.size for n in narratives]
        assert sum(sizes) == 30

    def test_assign_narratives_with_noise(self, sample_normalized_posts):
        """Test narrative assignment with noise label."""
        # Include noise labels (-1)
        labels = np.array([i // 10 if i < 25 else -1 for i in range(30)])
        similarities = np.random.rand(30)

        posts, narratives = assign_narratives(
            sample_normalized_posts,
            labels,
            similarities,
        )

        # Noise posts should be assigned to "noise" narrative
        noise_posts = [p for p in posts if p.narrative_id == "noise"]
        assert len(noise_posts) == 5

        # Narratives list should not include noise
        assert all(n.id != "noise" for n in narratives)


class TestKeywordExtraction:
    """Test keyword extraction."""

    def test_extract_tfidf_keywords(self):
        """Test TF-IDF keyword extraction."""
        texts = [
            "Python programming is great for data science",
            "Machine learning with Python is powerful",
            "Data science uses Python and machine learning",
        ]

        keywords = extract_tfidf_keywords(texts, top_k=5)

        assert len(keywords) <= 5
        assert "python" in keywords or "data" in keywords

    def test_extract_tfidf_keywords_empty(self):
        """Test TF-IDF with empty input."""
        keywords = extract_tfidf_keywords([])
        assert keywords == []

    def test_extract_frequency_keywords(self):
        """Test frequency-based keyword extraction."""
        texts = [
            "Python is great Python is powerful",
            "Python for data science",
        ]

        keywords = extract_frequency_keywords(texts, top_k=3)

        assert len(keywords) <= 3
        assert "python" in keywords

    def test_extract_frequency_keywords_stopwords(self):
        """Test that stopwords are removed."""
        texts = ["the and is are was were"]

        keywords = extract_frequency_keywords(texts, top_k=5)

        # All words are stopwords, should be empty or minimal
        assert len(keywords) == 0 or all(k not in ["the", "and", "is"] for k in keywords)
