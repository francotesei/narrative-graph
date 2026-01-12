"""Text embedding generation."""

from abc import ABC, abstractmethod

import numpy as np

from narrative_graph.config import get_settings
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers."""

    @abstractmethod
    def embed(self, texts: list[str]) -> np.ndarray:
        """Generate embeddings for texts.

        Args:
            texts: List of text strings

        Returns:
            Numpy array of embeddings (n_texts, embedding_dim)
        """
        pass

    @property
    @abstractmethod
    def dimension(self) -> int:
        """Get embedding dimension."""
        pass


class SentenceTransformerProvider(EmbeddingProvider):
    """Embedding provider using sentence-transformers."""

    def __init__(self, model_name: str | None = None, batch_size: int | None = None):
        """Initialize sentence transformer provider.

        Args:
            model_name: Model name from HuggingFace
            batch_size: Batch size for encoding
        """
        settings = get_settings()
        self.model_name = model_name or settings.embeddings.model
        self.batch_size = batch_size or settings.embeddings.batch_size
        self._model = None
        self._dimension: int | None = None

    @property
    def model(self):
        """Lazy load model."""
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(self.model_name)
            self._dimension = self._model.get_sentence_embedding_dimension()
            logger.info(
                "embedding_model_loaded",
                model=self.model_name,
                dimension=self._dimension,
            )
        return self._model

    @property
    def dimension(self) -> int:
        """Get embedding dimension."""
        if self._dimension is None:
            _ = self.model  # Trigger lazy load
        return self._dimension or 384  # Default for MiniLM

    def embed(self, texts: list[str]) -> np.ndarray:
        """Generate embeddings using sentence-transformers.

        Args:
            texts: List of text strings

        Returns:
            Numpy array of embeddings
        """
        if not texts:
            return np.array([])

        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            show_progress_bar=len(texts) > 100,
            convert_to_numpy=True,
        )

        return embeddings


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """Embedding provider using OpenAI API."""

    def __init__(
        self,
        model_name: str | None = None,
        api_key: str | None = None,
        batch_size: int = 100,
    ):
        """Initialize OpenAI provider.

        Args:
            model_name: OpenAI model name
            api_key: API key (or use OPENAI_API_KEY env var)
            batch_size: Batch size for API calls
        """
        settings = get_settings()
        self.model_name = model_name or settings.embeddings.openai_model
        self.api_key = api_key or settings.openai_api_key
        self.batch_size = batch_size
        self._client = None
        self._dimension = 1536  # Default for text-embedding-3-small

    @property
    def client(self):
        """Lazy load OpenAI client."""
        if self._client is None:
            from openai import OpenAI

            self._client = OpenAI(api_key=self.api_key)
            logger.info("openai_client_initialized", model=self.model_name)
        return self._client

    @property
    def dimension(self) -> int:
        """Get embedding dimension."""
        return self._dimension

    def embed(self, texts: list[str]) -> np.ndarray:
        """Generate embeddings using OpenAI API.

        Args:
            texts: List of text strings

        Returns:
            Numpy array of embeddings
        """
        if not texts:
            return np.array([])

        all_embeddings = []

        # Process in batches
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i : i + self.batch_size]

            response = self.client.embeddings.create(
                model=self.model_name,
                input=batch,
            )

            batch_embeddings = [item.embedding for item in response.data]
            all_embeddings.extend(batch_embeddings)

        return np.array(all_embeddings)


def get_embedding_provider(provider: str | None = None) -> EmbeddingProvider:
    """Get embedding provider based on configuration.

    Args:
        provider: Provider name ('sentence-transformers', 'openai')

    Returns:
        EmbeddingProvider instance
    """
    settings = get_settings()
    provider = provider or settings.embeddings.provider

    if provider == "sentence-transformers":
        return SentenceTransformerProvider()
    elif provider == "openai":
        if not settings.openai_api_key:
            logger.warning(
                "openai_key_missing",
                message="Falling back to sentence-transformers",
            )
            return SentenceTransformerProvider()
        return OpenAIEmbeddingProvider()
    else:
        logger.warning(
            "unknown_embedding_provider",
            provider=provider,
            fallback="sentence-transformers",
        )
        return SentenceTransformerProvider()


def generate_embeddings(
    texts: list[str],
    provider: EmbeddingProvider | None = None,
) -> np.ndarray:
    """Generate embeddings for texts.

    Args:
        texts: List of text strings
        provider: Optional embedding provider

    Returns:
        Numpy array of embeddings
    """
    if provider is None:
        provider = get_embedding_provider()

    logger.info("generating_embeddings", text_count=len(texts))
    embeddings = provider.embed(texts)
    logger.info(
        "embeddings_generated",
        count=len(embeddings),
        dimension=embeddings.shape[1] if len(embeddings) > 0 else 0,
    )

    return embeddings
