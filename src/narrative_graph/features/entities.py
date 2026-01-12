"""Entity extraction from text."""

from abc import ABC, abstractmethod

from narrative_graph.config import get_settings
from narrative_graph.ingestion.schemas import ExtractedEntity, NormalizedPost, PostEntities
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


class EntityExtractor(ABC):
    """Abstract base class for entity extractors."""

    @abstractmethod
    def extract(self, text: str) -> list[ExtractedEntity]:
        """Extract entities from text.

        Args:
            text: Text content

        Returns:
            List of extracted entities
        """
        pass

    def extract_batch(self, texts: list[str]) -> list[list[ExtractedEntity]]:
        """Extract entities from multiple texts.

        Args:
            texts: List of text contents

        Returns:
            List of entity lists
        """
        return [self.extract(text) for text in texts]


class SpacyEntityExtractor(EntityExtractor):
    """Entity extractor using spaCy NER."""

    def __init__(self, model_name: str | None = None, entity_types: list[str] | None = None):
        """Initialize spaCy extractor.

        Args:
            model_name: spaCy model name
            entity_types: List of entity types to extract
        """
        settings = get_settings()
        self.model_name = model_name or settings.entity_extraction.spacy_model
        self.entity_types = set(entity_types or settings.entity_extraction.entity_types)
        self._nlp = None

    @property
    def nlp(self):
        """Lazy load spaCy model."""
        if self._nlp is None:
            import spacy

            try:
                self._nlp = spacy.load(self.model_name)
                logger.info("spacy_model_loaded", model=self.model_name)
            except OSError:
                logger.warning(
                    "spacy_model_not_found",
                    model=self.model_name,
                    message="Run: python -m spacy download en_core_web_sm",
                )
                # Try to download
                from spacy.cli import download

                download(self.model_name)
                self._nlp = spacy.load(self.model_name)

        return self._nlp

    def extract(self, text: str) -> list[ExtractedEntity]:
        """Extract entities using spaCy.

        Args:
            text: Text content

        Returns:
            List of extracted entities
        """
        if not text:
            return []

        doc = self.nlp(text)
        entities = []

        for ent in doc.ents:
            if ent.label_ in self.entity_types:
                entities.append(
                    ExtractedEntity(
                        name=ent.text,
                        type=ent.label_,
                        start_char=ent.start_char,
                        end_char=ent.end_char,
                    )
                )

        return entities

    def extract_batch(self, texts: list[str]) -> list[list[ExtractedEntity]]:
        """Extract entities from multiple texts using pipe.

        Args:
            texts: List of text contents

        Returns:
            List of entity lists
        """
        results = []

        for doc in self.nlp.pipe(texts, batch_size=50):
            entities = []
            for ent in doc.ents:
                if ent.label_ in self.entity_types:
                    entities.append(
                        ExtractedEntity(
                            name=ent.text,
                            type=ent.label_,
                            start_char=ent.start_char,
                            end_char=ent.end_char,
                        )
                    )
            results.append(entities)

        return results


class RegexEntityExtractor(EntityExtractor):
    """Simple regex-based entity extractor as fallback."""

    def __init__(self):
        """Initialize regex patterns."""
        import re

        self.patterns = {
            "PERSON": re.compile(
                r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b"
            ),  # Capitalized multi-word
            "ORG": re.compile(
                r"\b([A-Z][A-Z]+(?:\s+[A-Z][A-Z]+)*)\b"
            ),  # All caps (acronyms)
        }

    def extract(self, text: str) -> list[ExtractedEntity]:
        """Extract entities using regex patterns.

        Args:
            text: Text content

        Returns:
            List of extracted entities
        """
        if not text:
            return []

        entities = []
        seen = set()

        for entity_type, pattern in self.patterns.items():
            for match in pattern.finditer(text):
                name = match.group(1)
                key = (name.lower(), entity_type)

                if key not in seen:
                    seen.add(key)
                    entities.append(
                        ExtractedEntity(
                            name=name,
                            type=entity_type,
                            start_char=match.start(1),
                            end_char=match.end(1),
                        )
                    )

        return entities


def get_entity_extractor(provider: str | None = None) -> EntityExtractor:
    """Get entity extractor based on configuration.

    Args:
        provider: Extractor provider ('spacy', 'regex')

    Returns:
        EntityExtractor instance
    """
    settings = get_settings()
    provider = provider or settings.entity_extraction.provider

    if provider == "spacy":
        return SpacyEntityExtractor()
    elif provider == "regex":
        return RegexEntityExtractor()
    else:
        logger.warning("unknown_entity_provider", provider=provider, fallback="regex")
        return RegexEntityExtractor()


def extract_entities(
    posts: list[NormalizedPost],
    extractor: EntityExtractor | None = None,
) -> list[PostEntities]:
    """Extract entities from all posts.

    Args:
        posts: List of posts
        extractor: Optional entity extractor instance

    Returns:
        List of PostEntities with extracted entities
    """
    if extractor is None:
        extractor = get_entity_extractor()

    logger.info("entity_extraction_started", post_count=len(posts))

    # Use text_clean if available, otherwise use text
    texts = [post.text_clean or post.text for post in posts]

    # Batch extract
    all_entities = extractor.extract_batch(texts)

    results = []
    total_entities = 0

    for post, entities in zip(posts, all_entities):
        results.append(PostEntities(post_id=post.id, entities=entities))
        total_entities += len(entities)

    logger.info(
        "entity_extraction_completed",
        post_count=len(posts),
        total_entities=total_entities,
    )

    return results
