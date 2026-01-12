"""Evidence generation for coordination detection."""

from typing import Any

from narrative_graph.ingestion.schemas import CoordinatedGroup, CoordinatedPair, CoordinationEvidence
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


def generate_evidence_summary(
    pairs: list[CoordinatedPair],
    groups: list[CoordinatedGroup],
) -> dict[str, Any]:
    """Generate a summary of coordination evidence.

    Args:
        pairs: List of coordinated pairs
        groups: List of coordinated groups

    Returns:
        Dictionary with evidence summary
    """
    summary: dict[str, Any] = {
        "total_pairs": len(pairs),
        "total_groups": len(groups),
        "top_groups": [],
        "most_coordinated_authors": [],
        "shared_indicators": {
            "domains": {},
            "hashtags": {},
        },
    }

    # Top groups by score
    sorted_groups = sorted(groups, key=lambda g: g.score, reverse=True)
    summary["top_groups"] = [
        {
            "id": g.id,
            "size": g.size,
            "score": round(g.score, 3),
            "author_ids": g.author_ids[:5],  # First 5 authors
            "narratives": g.narrative_ids,
        }
        for g in sorted_groups[:5]
    ]

    # Most coordinated authors
    author_scores: dict[str, list[float]] = {}
    for pair in pairs:
        if pair.author1_id not in author_scores:
            author_scores[pair.author1_id] = []
        if pair.author2_id not in author_scores:
            author_scores[pair.author2_id] = []
        author_scores[pair.author1_id].append(pair.score)
        author_scores[pair.author2_id].append(pair.score)

    author_avg_scores = [
        (author, sum(scores) / len(scores), len(scores))
        for author, scores in author_scores.items()
    ]
    author_avg_scores.sort(key=lambda x: (x[2], x[1]), reverse=True)

    summary["most_coordinated_authors"] = [
        {"author_id": a, "avg_score": round(s, 3), "pair_count": c}
        for a, s, c in author_avg_scores[:10]
    ]

    # Shared indicators
    domain_counts: dict[str, int] = {}
    hashtag_counts: dict[str, int] = {}

    for pair in pairs:
        for domain in pair.evidence.shared_domains:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        for hashtag in pair.evidence.shared_hashtags:
            hashtag_counts[hashtag] = hashtag_counts.get(hashtag, 0) + 1

    summary["shared_indicators"]["domains"] = dict(
        sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    )
    summary["shared_indicators"]["hashtags"] = dict(
        sorted(hashtag_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    )

    return summary


def format_pair_evidence(pair: CoordinatedPair) -> str:
    """Format evidence for a coordinated pair as human-readable text.

    Args:
        pair: Coordinated pair

    Returns:
        Formatted evidence string
    """
    lines = [
        f"Coordination between {pair.author1_id} and {pair.author2_id}",
        f"Score: {pair.score:.3f}",
    ]

    if pair.narrative_id:
        lines.append(f"Narrative: {pair.narrative_id}")

    evidence = pair.evidence

    if evidence.shared_domains:
        lines.append(f"Shared domains: {', '.join(evidence.shared_domains[:5])}")

    if evidence.shared_hashtags:
        lines.append(f"Shared hashtags: #{', #'.join(evidence.shared_hashtags[:5])}")

    if evidence.text_similarity is not None:
        lines.append(f"Text similarity: {evidence.text_similarity:.3f}")

    if evidence.time_delta_seconds is not None:
        minutes = evidence.time_delta_seconds / 60
        lines.append(f"Time difference: {minutes:.1f} minutes")

    if evidence.post_ids:
        lines.append(f"Example posts: {', '.join(evidence.post_ids[:4])}")

    return "\n".join(lines)


def format_group_evidence(group: CoordinatedGroup, pairs: list[CoordinatedPair]) -> str:
    """Format evidence for a coordination group as human-readable text.

    Args:
        group: Coordinated group
        pairs: All coordinated pairs (to find relevant ones)

    Returns:
        Formatted evidence string
    """
    lines = [
        f"Coordination Group: {group.id}",
        f"Size: {group.size} authors",
        f"Average Score: {group.score:.3f}",
        "",
        "Authors:",
    ]

    for author_id in group.author_ids[:10]:
        lines.append(f"  - {author_id}")

    if len(group.author_ids) > 10:
        lines.append(f"  ... and {len(group.author_ids) - 10} more")

    if group.narrative_ids:
        lines.append("")
        lines.append(f"Related narratives: {', '.join(group.narrative_ids)}")

    # Find relevant pairs
    group_authors = set(group.author_ids)
    relevant_pairs = [
        p for p in pairs
        if p.author1_id in group_authors and p.author2_id in group_authors
    ]

    if relevant_pairs:
        lines.append("")
        lines.append("Sample connections:")
        for pair in relevant_pairs[:3]:
            lines.append(f"  {pair.author1_id} <-> {pair.author2_id} (score: {pair.score:.3f})")

    return "\n".join(lines)
