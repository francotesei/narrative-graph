"""Configuration management for Narrative Graph Intelligence."""

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class PathsConfig(BaseModel):
    """Paths configuration."""

    data_dir: str = "data"
    outputs_dir: str = "outputs"
    bronze_dir: str = "outputs/bronze"
    silver_dir: str = "outputs/silver"
    features_dir: str = "outputs/features"
    dead_letter_dir: str = "outputs/dead_letter"


class Neo4jConfig(BaseModel):
    """Neo4j database configuration."""

    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: str = "narrative123"
    database: str = "neo4j"
    max_connection_pool_size: int = 50


class EmbeddingsConfig(BaseModel):
    """Embedding generation configuration."""

    provider: str = "sentence-transformers"
    model: str = "all-MiniLM-L6-v2"
    openai_model: str = "text-embedding-3-small"
    batch_size: int = 32


class ClusteringConfig(BaseModel):
    """Clustering configuration."""

    algorithm: str = "hdbscan"
    min_cluster_size: int = 5
    min_samples: int = 3
    metric: str = "euclidean"
    random_state: int = 42
    n_clusters: int = 10


class EntityExtractionConfig(BaseModel):
    """Entity extraction configuration."""

    provider: str = "spacy"
    spacy_model: str = "en_core_web_sm"
    entity_types: list[str] = Field(
        default_factory=lambda: ["PERSON", "ORG", "GPE", "LOC", "EVENT"]
    )


class CoordinationConfig(BaseModel):
    """Coordination detection configuration."""

    time_window_minutes: int = 60
    similarity_threshold: float = 0.85
    min_group_size: int = 3
    shared_domain_weight: float = 0.3
    shared_hashtag_weight: float = 0.2
    text_similarity_weight: float = 0.5


class RiskWeightsConfig(BaseModel):
    """Risk scoring weights."""

    velocity: float = 0.25
    coordination_density: float = 0.30
    bot_score: float = 0.20
    foreign_domain_ratio: float = 0.15
    toxicity: float = 0.10


class RiskThresholdsConfig(BaseModel):
    """Risk level thresholds."""

    low: float = 0.3
    medium: float = 0.6
    high: float = 0.8


class RiskConfig(BaseModel):
    """Risk scoring configuration."""

    weights: RiskWeightsConfig = Field(default_factory=RiskWeightsConfig)
    thresholds: RiskThresholdsConfig = Field(default_factory=RiskThresholdsConfig)
    foreign_tlds: list[str] = Field(default_factory=lambda: [".ru", ".cn", ".ir"])


class LLMConfig(BaseModel):
    """LLM configuration."""

    provider: str = "openai"
    model: str = "gpt-4o-mini"
    max_tokens: int = 500
    temperature: float = 0.3


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = "INFO"
    format: str = "json"
    log_file: str = "outputs/narrative_graph.log"


class PipelineConfig(BaseModel):
    """Pipeline configuration."""

    run_id_prefix: str = "run"
    save_intermediate: bool = True


class Settings(BaseSettings):
    """Main settings class combining all configurations."""

    paths: PathsConfig = Field(default_factory=PathsConfig)
    neo4j: Neo4jConfig = Field(default_factory=Neo4jConfig)
    embeddings: EmbeddingsConfig = Field(default_factory=EmbeddingsConfig)
    clustering: ClusteringConfig = Field(default_factory=ClusteringConfig)
    entity_extraction: EntityExtractionConfig = Field(default_factory=EntityExtractionConfig)
    coordination: CoordinationConfig = Field(default_factory=CoordinationConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)

    # Environment variables
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    neo4j_password: str | None = Field(default=None, alias="NEO4J_PASSWORD")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


def _resolve_env_vars(value: Any) -> Any:
    """Resolve environment variable references in config values."""
    if isinstance(value, str):
        # Match ${VAR:-default} or ${VAR} patterns
        pattern = r"\$\{([^}:]+)(?::-([^}]*))?\}"
        matches = re.findall(pattern, value)
        for var_name, default in matches:
            env_value = os.environ.get(var_name, default)
            value = re.sub(
                rf"\$\{{{var_name}(?::-[^}}]*)?\}}",
                env_value if env_value else "",
                value,
            )
        return value
    elif isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def load_config(config_path: str | Path | None = None) -> Settings:
    """Load configuration from YAML file and environment variables.

    Args:
        config_path: Path to config.yaml file. If None, looks for configs/config.yaml

    Returns:
        Settings object with all configuration
    """
    if config_path is None:
        # Look for config in standard locations
        possible_paths = [
            Path("configs/config.yaml"),
            Path("config.yaml"),
            Path(__file__).parent.parent.parent.parent / "configs" / "config.yaml",
        ]
        for path in possible_paths:
            if path.exists():
                config_path = path
                break

    config_data = {}
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            config_data = yaml.safe_load(f) or {}
        # Resolve environment variable references
        config_data = _resolve_env_vars(config_data)

    # Create settings with config data
    settings = Settings(**config_data)

    # Override neo4j password from environment if set
    if settings.neo4j_password:
        settings.neo4j.password = settings.neo4j_password

    return settings


# Global settings instance (lazy loaded)
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get the global settings instance."""
    global _settings
    if _settings is None:
        _settings = load_config()
    return _settings


def reset_settings() -> None:
    """Reset the global settings instance (useful for testing)."""
    global _settings
    _settings = None
