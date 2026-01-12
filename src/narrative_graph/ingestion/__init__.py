"""Data ingestion module."""

from narrative_graph.ingestion.schemas import (
    RawPost,
    NormalizedPost,
    DeadLetterRecord,
)
from narrative_graph.ingestion.loaders import load_jsonl, load_csv, load_data
from narrative_graph.ingestion.normalizer import normalize_posts, normalize_post

__all__ = [
    "RawPost",
    "NormalizedPost",
    "DeadLetterRecord",
    "load_jsonl",
    "load_csv",
    "load_data",
    "normalize_posts",
    "normalize_post",
]
