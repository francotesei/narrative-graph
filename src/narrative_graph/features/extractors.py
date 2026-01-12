"""Feature extraction from posts."""

from narrative_graph.features.text import clean_text, detect_language
from narrative_graph.ingestion.schemas import NormalizedPost
from narrative_graph.logging import get_logger
from narrative_graph.storage.parquet import ParquetStorage

logger = get_logger(__name__)


def extract_features(
    posts: list[NormalizedPost],
    run_id: str | None = None,
    storage: ParquetStorage | None = None,
    detect_lang: bool = True,
) -> list[NormalizedPost]:
    """Extract and enrich features for posts.

    Args:
        posts: List of normalized posts
        run_id: Optional run ID for saving outputs
        storage: Optional parquet storage
        detect_lang: Whether to detect language if missing

    Returns:
        List of posts with enriched features
    """
    logger.info("feature_extraction_started", post_count=len(posts))

    enriched_posts = []

    for post in posts:
        # Clean text
        post.text_clean = clean_text(post.text, remove_urls=True)

        # Detect language if missing
        if detect_lang and not post.lang:
            post.lang = detect_language(post.text)

        # Update counts (already set during normalization, but verify)
        post.text_length = len(post.text)
        post.url_count = len(post.urls)
        post.hashtag_count = len(post.hashtags)
        post.mention_count = len(post.mentions)

        enriched_posts.append(post)

    logger.info("feature_extraction_completed", post_count=len(enriched_posts))

    # Save features dataset
    if storage and run_id:
        records = [post.model_dump(mode="json") for post in enriched_posts]
        storage.save_records(records, "features", run_id)

    return enriched_posts


def compute_author_features(posts: list[NormalizedPost]) -> dict[str, dict]:
    """Compute aggregated features per author.

    Args:
        posts: List of posts

    Returns:
        Dictionary mapping author_id to feature dict
    """
    author_features: dict[str, dict] = {}

    for post in posts:
        author_id = post.author_id

        if author_id not in author_features:
            author_features[author_id] = {
                "author_id": author_id,
                "handle": post.author_handle,
                "platform": post.platform.value,
                "post_count": 0,
                "total_urls": 0,
                "total_hashtags": 0,
                "total_mentions": 0,
                "unique_domains": set(),
                "unique_hashtags": set(),
                "timestamps": [],
            }

        features = author_features[author_id]
        features["post_count"] += 1
        features["total_urls"] += post.url_count
        features["total_hashtags"] += post.hashtag_count
        features["total_mentions"] += post.mention_count
        features["unique_domains"].update(post.domains)
        features["unique_hashtags"].update(post.hashtags)
        features["timestamps"].append(post.timestamp)

    # Convert sets to counts
    for author_id, features in author_features.items():
        features["unique_domain_count"] = len(features.pop("unique_domains"))
        features["unique_hashtag_count"] = len(features.pop("unique_hashtags"))

        # Calculate posting frequency
        timestamps = sorted(features.pop("timestamps"))
        if len(timestamps) > 1:
            time_span = (timestamps[-1] - timestamps[0]).total_seconds()
            features["posts_per_hour"] = (
                features["post_count"] / (time_span / 3600) if time_span > 0 else 0
            )
        else:
            features["posts_per_hour"] = 0

    return author_features


def compute_domain_features(posts: list[NormalizedPost]) -> dict[str, dict]:
    """Compute aggregated features per domain.

    Args:
        posts: List of posts

    Returns:
        Dictionary mapping domain to feature dict
    """
    domain_features: dict[str, dict] = {}

    for post in posts:
        for domain in post.domains:
            if domain not in domain_features:
                domain_features[domain] = {
                    "domain": domain,
                    "post_count": 0,
                    "unique_authors": set(),
                    "platforms": set(),
                }

            features = domain_features[domain]
            features["post_count"] += 1
            features["unique_authors"].add(post.author_id)
            features["platforms"].add(post.platform.value)

    # Convert sets to counts
    for domain, features in domain_features.items():
        features["unique_author_count"] = len(features.pop("unique_authors"))
        features["platform_count"] = len(features.pop("platforms"))

    return domain_features


def compute_hashtag_features(posts: list[NormalizedPost]) -> dict[str, dict]:
    """Compute aggregated features per hashtag.

    Args:
        posts: List of posts

    Returns:
        Dictionary mapping hashtag to feature dict
    """
    hashtag_features: dict[str, dict] = {}

    for post in posts:
        for hashtag in post.hashtags:
            if hashtag not in hashtag_features:
                hashtag_features[hashtag] = {
                    "hashtag": hashtag,
                    "post_count": 0,
                    "unique_authors": set(),
                    "platforms": set(),
                }

            features = hashtag_features[hashtag]
            features["post_count"] += 1
            features["unique_authors"].add(post.author_id)
            features["platforms"].add(post.platform.value)

    # Convert sets to counts
    for hashtag, features in hashtag_features.items():
        features["unique_author_count"] = len(features.pop("unique_authors"))
        features["platform_count"] = len(features.pop("platforms"))

    return hashtag_features
