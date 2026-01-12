"""Clustering for narrative detection."""

from collections import defaultdict
from datetime import datetime

import numpy as np

from narrative_graph.config import get_settings
from narrative_graph.ingestion.schemas import NarrativeMetadata, NormalizedPost
from narrative_graph.logging import get_logger
from narrative_graph.narratives.embeddings import generate_embeddings, get_embedding_provider

logger = get_logger(__name__)


def cluster_hdbscan(
    embeddings: np.ndarray,
    min_cluster_size: int = 5,
    min_samples: int = 3,
    metric: str = "euclidean",
) -> tuple[np.ndarray, np.ndarray]:
    """Cluster embeddings using HDBSCAN.

    Args:
        embeddings: Embedding matrix (n_samples, n_features)
        min_cluster_size: Minimum cluster size
        min_samples: Minimum samples for core points
        metric: Distance metric

    Returns:
        Tuple of (cluster_labels, probabilities)
    """
    import hdbscan

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric=metric,
        core_dist_n_jobs=-1,
    )

    cluster_labels = clusterer.fit_predict(embeddings)
    probabilities = clusterer.probabilities_

    return cluster_labels, probabilities


def cluster_kmeans(
    embeddings: np.ndarray,
    n_clusters: int = 10,
    random_state: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    """Cluster embeddings using KMeans.

    Args:
        embeddings: Embedding matrix (n_samples, n_features)
        n_clusters: Number of clusters
        random_state: Random seed for reproducibility

    Returns:
        Tuple of (cluster_labels, distances_to_centroid)
    """
    from sklearn.cluster import KMeans

    kmeans = KMeans(
        n_clusters=n_clusters,
        random_state=random_state,
        n_init=10,
    )

    cluster_labels = kmeans.fit_predict(embeddings)

    # Calculate distance to centroid as "probability" proxy
    distances = kmeans.transform(embeddings)
    min_distances = distances.min(axis=1)

    # Convert to similarity (inverse distance, normalized)
    max_dist = min_distances.max() if min_distances.max() > 0 else 1
    similarities = 1 - (min_distances / max_dist)

    return cluster_labels, similarities


def cluster_posts(
    posts: list[NormalizedPost],
    algorithm: str | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Cluster posts into narratives.

    Args:
        posts: List of normalized posts
        algorithm: Clustering algorithm ('hdbscan' or 'kmeans')

    Returns:
        Tuple of (embeddings, cluster_labels, similarities)
    """
    settings = get_settings()
    algorithm = algorithm or settings.clustering.algorithm

    logger.info("clustering_started", algorithm=algorithm, post_count=len(posts))

    # Generate embeddings
    texts = [post.text_clean or post.text for post in posts]
    embeddings = generate_embeddings(texts)

    # Cluster
    if algorithm == "hdbscan":
        labels, similarities = cluster_hdbscan(
            embeddings,
            min_cluster_size=settings.clustering.min_cluster_size,
            min_samples=settings.clustering.min_samples,
            metric=settings.clustering.metric,
        )
    elif algorithm == "kmeans":
        labels, similarities = cluster_kmeans(
            embeddings,
            n_clusters=settings.clustering.n_clusters,
            random_state=settings.clustering.random_state,
        )
    else:
        raise ValueError(f"Unknown clustering algorithm: {algorithm}")

    # Count clusters (excluding noise label -1)
    unique_labels = set(labels)
    n_clusters = len([l for l in unique_labels if l >= 0])
    n_noise = sum(1 for l in labels if l == -1)

    logger.info(
        "clustering_completed",
        n_clusters=n_clusters,
        n_noise=n_noise,
        total_posts=len(posts),
    )

    return embeddings, labels, similarities


def assign_narratives(
    posts: list[NormalizedPost],
    labels: np.ndarray,
    similarities: np.ndarray,
    embeddings: np.ndarray | None = None,
) -> tuple[list[NormalizedPost], list[NarrativeMetadata]]:
    """Assign narrative IDs to posts and create narrative metadata.

    Args:
        posts: List of posts
        labels: Cluster labels
        similarities: Similarity scores
        embeddings: Optional embeddings to store

    Returns:
        Tuple of (updated posts, narrative metadata list)
    """
    # Group posts by cluster
    cluster_posts: dict[int, list[tuple[NormalizedPost, float, int]]] = defaultdict(list)

    for idx, (post, label, sim) in enumerate(zip(posts, labels, similarities)):
        cluster_posts[int(label)].append((post, float(sim), idx))

    narratives: list[NarrativeMetadata] = []
    updated_posts: list[NormalizedPost] = []

    for label, post_data in cluster_posts.items():
        # Generate narrative ID
        if label == -1:
            narrative_id = "noise"
        else:
            narrative_id = f"narrative_{label:04d}"

        # Collect metadata
        cluster_posts_list = [p for p, _, _ in post_data]
        timestamps = [p.timestamp for p in cluster_posts_list]
        platforms = list(set(p.platform.value for p in cluster_posts_list))
        authors = set(p.author_id for p in cluster_posts_list)

        # Aggregate domains and hashtags
        all_domains: dict[str, int] = defaultdict(int)
        all_hashtags: dict[str, int] = defaultdict(int)

        for p in cluster_posts_list:
            for d in p.domains:
                all_domains[d] += 1
            for h in p.hashtags:
                all_hashtags[h] += 1

        top_domains = sorted(all_domains.keys(), key=lambda x: all_domains[x], reverse=True)[:5]
        top_hashtags = sorted(all_hashtags.keys(), key=lambda x: all_hashtags[x], reverse=True)[
            :5
        ]

        # Create narrative metadata (keywords will be added later)
        if label != -1:
            narrative = NarrativeMetadata(
                id=narrative_id,
                size=len(cluster_posts_list),
                top_domains=top_domains,
                top_hashtags=top_hashtags,
                start_time=min(timestamps) if timestamps else None,
                end_time=max(timestamps) if timestamps else None,
                platforms=platforms,
                author_count=len(authors),
            )
            narratives.append(narrative)

        # Update posts with narrative assignment
        for post, sim, idx in post_data:
            post.narrative_id = narrative_id
            post.cluster_similarity = sim
            if embeddings is not None:
                post.embedding = embeddings[idx].tolist()
            updated_posts.append(post)

    # Sort narratives by size
    narratives.sort(key=lambda n: n.size, reverse=True)

    logger.info(
        "narratives_assigned",
        narrative_count=len(narratives),
        post_count=len(updated_posts),
    )

    return updated_posts, narratives
