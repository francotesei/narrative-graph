"""Narrative detection module."""

from narrative_graph.narratives.embeddings import (
    EmbeddingProvider,
    SentenceTransformerProvider,
    generate_embeddings,
)
from narrative_graph.narratives.clustering import (
    cluster_posts,
    assign_narratives,
)
from narrative_graph.narratives.keywords import extract_narrative_keywords

__all__ = [
    "EmbeddingProvider",
    "SentenceTransformerProvider",
    "generate_embeddings",
    "cluster_posts",
    "assign_narratives",
    "extract_narrative_keywords",
]
