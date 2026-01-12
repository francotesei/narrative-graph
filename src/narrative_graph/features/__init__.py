"""Feature extraction module."""

from narrative_graph.features.text import clean_text, detect_language
from narrative_graph.features.extractors import extract_features
from narrative_graph.features.entities import (
    EntityExtractor,
    SpacyEntityExtractor,
    RegexEntityExtractor,
    get_entity_extractor,
    extract_entities,
)

__all__ = [
    "clean_text",
    "detect_language",
    "extract_features",
    "EntityExtractor",
    "SpacyEntityExtractor",
    "RegexEntityExtractor",
    "get_entity_extractor",
    "extract_entities",
]
