"""Individual risk score components."""

from collections import defaultdict
from datetime import datetime, timedelta

from narrative_graph.config import get_settings
from narrative_graph.ingestion.schemas import (
    CoordinatedGroup,
    NarrativeMetadata,
    NormalizedPost,
)
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


def calculate_velocity_score(
    posts: list[NormalizedPost],
    narrative: NarrativeMetadata,
) -> float:
    """Calculate velocity score based on posting rate.

    Higher velocity = higher risk (potential coordinated campaign)

    Args:
        posts: Posts in the narrative
        narrative: Narrative metadata

    Returns:
        Velocity score (0-1)
    """
    if not posts or len(posts) < 2:
        return 0.0

    # Get time range
    timestamps = sorted([p.timestamp for p in posts])
    time_span = (timestamps[-1] - timestamps[0]).total_seconds()

    if time_span <= 0:
        return 1.0  # All posts at same time = suspicious

    # Calculate posts per hour
    hours = time_span / 3600
    posts_per_hour = len(posts) / hours

    # Normalize: 10+ posts/hour = max score
    # Using sigmoid-like normalization
    score = min(posts_per_hour / 10, 1.0)

    # Boost if concentrated in short bursts
    # Check for burst patterns (many posts in short windows)
    burst_count = 0
    window = timedelta(minutes=15)

    for i, ts in enumerate(timestamps):
        window_posts = sum(
            1 for other_ts in timestamps[i:]
            if other_ts - ts <= window
        )
        if window_posts >= 5:
            burst_count += 1

    burst_ratio = burst_count / len(timestamps) if timestamps else 0
    score = min(score + burst_ratio * 0.2, 1.0)

    return round(score, 4)


def calculate_coordination_score(
    narrative_id: str,
    groups: list[CoordinatedGroup],
    total_authors: int,
) -> float:
    """Calculate coordination density score.

    Higher coordination = higher risk

    Args:
        narrative_id: Narrative identifier
        groups: Coordinated groups detected
        total_authors: Total authors in narrative

    Returns:
        Coordination score (0-1)
    """
    if total_authors <= 1:
        return 0.0

    # Find groups related to this narrative
    relevant_groups = [
        g for g in groups
        if narrative_id in g.narrative_ids
    ]

    if not relevant_groups:
        return 0.0

    # Count coordinated authors
    coordinated_authors = set()
    total_group_score = 0.0

    for group in relevant_groups:
        coordinated_authors.update(group.author_ids)
        total_group_score += group.score * group.size

    # Ratio of coordinated authors
    coordination_ratio = len(coordinated_authors) / total_authors

    # Weight by average group score
    avg_group_score = total_group_score / len(coordinated_authors) if coordinated_authors else 0

    # Combined score
    score = coordination_ratio * 0.6 + avg_group_score * 0.4

    return round(min(score, 1.0), 4)


def calculate_foreign_domain_score(
    posts: list[NormalizedPost],
    foreign_tlds: list[str] | None = None,
) -> float:
    """Calculate foreign domain ratio score.

    Higher foreign domain ratio = higher risk

    Args:
        posts: Posts in the narrative
        foreign_tlds: List of foreign TLDs to check

    Returns:
        Foreign domain score (0-1)
    """
    settings = get_settings()
    foreign_tlds = foreign_tlds or settings.risk.foreign_tlds

    if not posts:
        return 0.0

    # Count domains
    all_domains: set[str] = set()
    foreign_domains: set[str] = set()

    for post in posts:
        for domain in post.domains:
            all_domains.add(domain)
            # Check if domain ends with foreign TLD
            for tld in foreign_tlds:
                if domain.endswith(tld):
                    foreign_domains.add(domain)
                    break

    if not all_domains:
        return 0.0

    ratio = len(foreign_domains) / len(all_domains)

    return round(ratio, 4)


def calculate_bot_score(
    posts: list[NormalizedPost],
) -> float:
    """Calculate bot-like behavior score using heuristics.

    Args:
        posts: Posts in the narrative

    Returns:
        Bot score (0-1)
    """
    if not posts:
        return 0.0

    # Group by author
    author_posts: dict[str, list[NormalizedPost]] = defaultdict(list)
    for post in posts:
        author_posts[post.author_id].append(post)

    bot_indicators = []

    for author_id, author_posts_list in author_posts.items():
        if len(author_posts_list) < 2:
            continue

        indicators = 0.0

        # High posting frequency
        timestamps = sorted([p.timestamp for p in author_posts_list])
        time_span = (timestamps[-1] - timestamps[0]).total_seconds()
        if time_span > 0:
            posts_per_hour = len(author_posts_list) / (time_span / 3600)
            if posts_per_hour > 20:
                indicators += 0.3

        # Similar text patterns
        texts = [p.text for p in author_posts_list]
        if len(texts) >= 3:
            # Check for repetitive content
            unique_ratio = len(set(texts)) / len(texts)
            if unique_ratio < 0.5:
                indicators += 0.3

        # Consistent posting intervals (bot-like regularity)
        if len(timestamps) >= 3:
            intervals = [
                (timestamps[i + 1] - timestamps[i]).total_seconds()
                for i in range(len(timestamps) - 1)
            ]
            if intervals:
                avg_interval = sum(intervals) / len(intervals)
                variance = sum((i - avg_interval) ** 2 for i in intervals) / len(intervals)
                # Low variance = regular posting = bot-like
                if avg_interval > 0 and variance / avg_interval < 0.1:
                    indicators += 0.2

        # High URL ratio
        url_ratio = sum(1 for p in author_posts_list if p.urls) / len(author_posts_list)
        if url_ratio > 0.8:
            indicators += 0.2

        bot_indicators.append(min(indicators, 1.0))

    if not bot_indicators:
        return 0.0

    return round(sum(bot_indicators) / len(bot_indicators), 4)


def calculate_toxicity_score(
    posts: list[NormalizedPost],
) -> float:
    """Calculate toxicity score using simple heuristics.

    Note: For production, use a proper toxicity model.

    Args:
        posts: Posts in the narrative

    Returns:
        Toxicity score (0-1)
    """
    if not posts:
        return 0.0

    # Simple keyword-based toxicity detection
    # In production, use a proper model like Perspective API
    toxic_keywords = {
        "hate", "kill", "die", "attack", "destroy", "enemy", "threat",
        "dangerous", "evil", "corrupt", "conspiracy", "hoax", "fake",
        "propaganda", "lies", "traitor", "invasion", "war",
    }

    toxic_count = 0
    total_words = 0

    for post in posts:
        text_lower = post.text.lower()
        words = text_lower.split()
        total_words += len(words)

        for word in words:
            # Strip punctuation
            clean_word = "".join(c for c in word if c.isalnum())
            if clean_word in toxic_keywords:
                toxic_count += 1

    if total_words == 0:
        return 0.0

    # Normalize: more than 5% toxic words = max score
    ratio = toxic_count / total_words
    score = min(ratio / 0.05, 1.0)

    return round(score, 4)
