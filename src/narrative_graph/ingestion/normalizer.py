"""Data normalization from raw to silver layer."""

import json
import re
from pathlib import Path
from urllib.parse import urlparse

from narrative_graph.config import get_settings
from narrative_graph.ingestion.loaders import load_data, parse_raw_post, count_records
from narrative_graph.ingestion.schemas import (
    NormalizedPost,
    Platform,
    RawPost,
    DeadLetterRecord,
)
from narrative_graph.logging import get_logger
from narrative_graph.storage.database import RunDatabase
from narrative_graph.storage.parquet import ParquetStorage

logger = get_logger(__name__)


def normalize_platform(platform: str) -> Platform:
    """Normalize platform string to Platform enum.

    Args:
        platform: Raw platform string

    Returns:
        Platform enum value
    """
    platform_lower = platform.lower().strip()
    platform_map = {
        "twitter": Platform.TWITTER,
        "x": Platform.TWITTER,
        "reddit": Platform.REDDIT,
        "news": Platform.NEWS,
        "facebook": Platform.FACEBOOK,
        "fb": Platform.FACEBOOK,
        "telegram": Platform.TELEGRAM,
        "tg": Platform.TELEGRAM,
    }
    return platform_map.get(platform_lower, Platform.OTHER)


def extract_domain(url: str) -> str | None:
    """Extract domain from URL.

    Args:
        url: URL string

    Returns:
        Domain string or None if invalid
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Remove www. prefix
        if domain.startswith("www."):
            domain = domain[4:]
        return domain if domain else None
    except Exception:
        return None


def extract_urls_from_text(text: str) -> list[str]:
    """Extract URLs from text content.

    Args:
        text: Text content

    Returns:
        List of URLs found
    """
    url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
    return re.findall(url_pattern, text)


def extract_hashtags_from_text(text: str) -> list[str]:
    """Extract hashtags from text content.

    Args:
        text: Text content

    Returns:
        List of hashtags (without # prefix)
    """
    hashtag_pattern = r"#(\w+)"
    return re.findall(hashtag_pattern, text)


def extract_mentions_from_text(text: str) -> list[str]:
    """Extract @mentions from text content.

    Args:
        text: Text content

    Returns:
        List of mentions (without @ prefix)
    """
    mention_pattern = r"@(\w+)"
    return re.findall(mention_pattern, text)


def normalize_post(raw_post: RawPost) -> NormalizedPost:
    """Normalize a raw post to silver layer format.

    Args:
        raw_post: Raw post from ingestion

    Returns:
        Normalized post
    """
    # Normalize platform
    platform = normalize_platform(raw_post.platform)

    # Combine explicit URLs with those extracted from text
    urls = list(raw_post.urls or [])
    text_urls = extract_urls_from_text(raw_post.text)
    all_urls = list(set(urls + text_urls))

    # Extract domains
    domains = [d for url in all_urls if (d := extract_domain(url))]
    domains = list(set(domains))

    # Combine explicit hashtags with those extracted from text
    hashtags = list(raw_post.hashtags or [])
    text_hashtags = extract_hashtags_from_text(raw_post.text)
    all_hashtags = list(set(h.lower() for h in (hashtags + text_hashtags)))

    # Combine explicit mentions with those extracted from text
    mentions = list(raw_post.mentions or [])
    text_mentions = extract_mentions_from_text(raw_post.text)
    all_mentions = list(set(m.lower() for m in (mentions + text_mentions)))

    # Create normalized post
    return NormalizedPost(
        id=raw_post.id,
        timestamp=raw_post.timestamp,  # type: ignore (already parsed)
        platform=platform,
        author_id=raw_post.author_id,
        author_handle=raw_post.author_handle,
        text=raw_post.text,
        lang=raw_post.lang,
        urls=all_urls,
        domains=domains,
        hashtags=all_hashtags,
        mentions=all_mentions,
        metadata=raw_post.metadata or {},
        text_length=len(raw_post.text),
        url_count=len(all_urls),
        hashtag_count=len(all_hashtags),
        mention_count=len(all_mentions),
    )


def normalize_posts(
    input_file: str | Path,
    run_id: str,
    db: RunDatabase | None = None,
    storage: ParquetStorage | None = None,
) -> tuple[list[NormalizedPost], int]:
    """Normalize all posts from an input file.

    Args:
        input_file: Path to input data file
        run_id: Pipeline run identifier
        db: Optional database for dead letter tracking
        storage: Optional parquet storage for outputs

    Returns:
        Tuple of (list of normalized posts, dead letter count)
    """
    settings = get_settings()
    input_file = Path(input_file)

    if db is None:
        db = RunDatabase()
    if storage is None:
        storage = ParquetStorage()

    normalized_posts: list[NormalizedPost] = []
    dead_letter_count = 0

    # Count total records for progress logging
    total_records = count_records(input_file)
    logger.info("normalization_started", input_file=str(input_file), total_records=total_records)

    for line_num, record, load_error in load_data(input_file):
        if load_error:
            # Record failed to load
            db.add_dead_letter(
                run_id=run_id,
                raw_payload=json.dumps({"line": line_num, "error": load_error}),
                error_type="LoadError",
                error_message=load_error,
                source_file=str(input_file),
                line_number=line_num,
            )
            dead_letter_count += 1
            continue

        if record is None:
            continue

        # Parse raw post
        raw_post, parse_error = parse_raw_post(
            record, line_number=line_num, source_file=str(input_file)
        )

        if parse_error:
            db.add_dead_letter(
                run_id=run_id,
                raw_payload=json.dumps(record, default=str),
                error_type="ValidationError",
                error_message=parse_error,
                source_file=str(input_file),
                line_number=line_num,
            )
            dead_letter_count += 1
            continue

        if raw_post is None:
            continue

        # Normalize post
        try:
            normalized = normalize_post(raw_post)
            normalized_posts.append(normalized)
        except Exception as e:
            db.add_dead_letter(
                run_id=run_id,
                raw_payload=json.dumps(record, default=str),
                error_type="NormalizationError",
                error_message=str(e),
                source_file=str(input_file),
                line_number=line_num,
            )
            dead_letter_count += 1

    logger.info(
        "normalization_completed",
        normalized_count=len(normalized_posts),
        dead_letter_count=dead_letter_count,
    )

    # Save bronze (raw) data
    if settings.pipeline.save_intermediate:
        bronze_records = []
        for line_num, record, _ in load_data(input_file):
            if record:
                bronze_records.append(record)
        if bronze_records:
            storage.save_records(bronze_records, "bronze", run_id)

    # Save silver (normalized) data
    silver_records = [post.model_dump(mode="json") for post in normalized_posts]
    if silver_records:
        storage.save_records(silver_records, "silver", run_id)

    return normalized_posts, dead_letter_count
