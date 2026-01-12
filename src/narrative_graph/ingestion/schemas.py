"""Pydantic schemas for data ingestion and domain models."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class Platform(str, Enum):
    """Supported platforms."""

    TWITTER = "twitter"
    REDDIT = "reddit"
    NEWS = "news"
    FACEBOOK = "facebook"
    TELEGRAM = "telegram"
    OTHER = "other"


class RiskLevel(str, Enum):
    """Risk level classification."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


# =============================================================================
# Input Schemas (Raw Data)
# =============================================================================


class RawPost(BaseModel):
    """Raw post as received from input data."""

    id: str
    timestamp: str | datetime
    platform: str
    author_id: str
    author_handle: str | None = None
    text: str
    lang: str | None = None
    urls: list[str] | None = None
    hashtags: list[str] | None = None
    mentions: list[str] | None = None
    metadata: dict[str, Any] | None = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            # Try ISO format first
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                pass
            # Try common formats
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                try:
                    return datetime.strptime(v, fmt)
                except ValueError:
                    continue
        raise ValueError(f"Cannot parse timestamp: {v}")


# =============================================================================
# Normalized Schemas (Silver Layer)
# =============================================================================


class NormalizedPost(BaseModel):
    """Normalized post after ingestion processing."""

    id: str
    timestamp: datetime
    platform: Platform
    author_id: str
    author_handle: str | None = None
    text: str
    text_clean: str | None = None
    lang: str | None = None
    urls: list[str] = Field(default_factory=list)
    domains: list[str] = Field(default_factory=list)
    hashtags: list[str] = Field(default_factory=list)
    mentions: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    # Features (populated during enrichment)
    text_length: int = 0
    url_count: int = 0
    hashtag_count: int = 0
    mention_count: int = 0

    # Clustering results (populated during narrative detection)
    narrative_id: str | None = None
    cluster_similarity: float | None = None
    embedding: list[float] | None = None


class DeadLetterRecord(BaseModel):
    """Record for failed ingestion attempts."""

    raw_payload: dict[str, Any]
    error_type: str
    error_message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source_file: str | None = None
    line_number: int | None = None


# =============================================================================
# Entity Schemas
# =============================================================================


class ExtractedEntity(BaseModel):
    """Entity extracted from text."""

    name: str
    type: str  # PERSON, ORG, GPE, LOC, EVENT, etc.
    start_char: int | None = None
    end_char: int | None = None
    confidence: float | None = None


class PostEntities(BaseModel):
    """Entities extracted from a post."""

    post_id: str
    entities: list[ExtractedEntity] = Field(default_factory=list)


# =============================================================================
# Narrative Schemas
# =============================================================================


class NarrativeMetadata(BaseModel):
    """Metadata for a detected narrative cluster."""

    id: str
    size: int
    keywords: list[str] = Field(default_factory=list)
    top_domains: list[str] = Field(default_factory=list)
    top_hashtags: list[str] = Field(default_factory=list)
    top_entities: list[str] = Field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None
    platforms: list[str] = Field(default_factory=list)
    author_count: int = 0


# =============================================================================
# Coordination Schemas
# =============================================================================


class CoordinationEvidence(BaseModel):
    """Evidence for coordination detection."""

    post_ids: list[str] = Field(default_factory=list)
    shared_domains: list[str] = Field(default_factory=list)
    shared_hashtags: list[str] = Field(default_factory=list)
    text_similarity: float | None = None
    time_delta_seconds: float | None = None


class CoordinatedPair(BaseModel):
    """A pair of coordinated authors."""

    author1_id: str
    author2_id: str
    score: float
    evidence: CoordinationEvidence
    narrative_id: str | None = None


class CoordinatedGroup(BaseModel):
    """A group of coordinated authors."""

    id: str
    author_ids: list[str]
    score: float
    evidence_summary: str
    narrative_ids: list[str] = Field(default_factory=list)
    size: int = 0


# =============================================================================
# Risk Schemas
# =============================================================================


class RiskComponents(BaseModel):
    """Individual components of risk score."""

    velocity: float = 0.0
    coordination_density: float = 0.0
    bot_score: float = 0.0
    foreign_domain_ratio: float = 0.0
    toxicity: float = 0.0


class NarrativeRisk(BaseModel):
    """Risk assessment for a narrative."""

    narrative_id: str
    risk_score: float
    risk_level: RiskLevel
    components: RiskComponents
    reasons: list[str] = Field(default_factory=list)


# =============================================================================
# Explanation Schemas
# =============================================================================


class Explanation(BaseModel):
    """Generated explanation for a narrative or coordination group."""

    target_id: str
    target_type: str  # "narrative" or "coordination_group"
    explanation_text: str
    facts_used: dict[str, Any] = Field(default_factory=dict)
    model_info: str | None = None
    generated_at: datetime = Field(default_factory=datetime.utcnow)


# =============================================================================
# Author Schema
# =============================================================================


class Author(BaseModel):
    """Author/account information."""

    id: str
    handle: str | None = None
    platform: Platform
    post_count: int = 0
    coordination_score: float = 0.0
    degree_centrality: float = 0.0
