"""Keyword extraction for narratives."""

from collections import defaultdict

from sklearn.feature_extraction.text import TfidfVectorizer

from narrative_graph.features.text import remove_stopwords, tokenize_simple
from narrative_graph.ingestion.schemas import NarrativeMetadata, NormalizedPost
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


def extract_tfidf_keywords(
    texts: list[str],
    top_k: int = 10,
    max_features: int = 1000,
    ngram_range: tuple[int, int] = (1, 2),
) -> list[str]:
    """Extract top keywords using TF-IDF.

    Args:
        texts: List of text documents
        top_k: Number of top keywords to return
        max_features: Maximum vocabulary size
        ngram_range: N-gram range for vectorizer

    Returns:
        List of top keywords
    """
    if not texts:
        return []

    try:
        vectorizer = TfidfVectorizer(
            max_features=max_features,
            ngram_range=ngram_range,
            stop_words="english",
            lowercase=True,
        )

        tfidf_matrix = vectorizer.fit_transform(texts)
        feature_names = vectorizer.get_feature_names_out()

        # Sum TF-IDF scores across all documents
        tfidf_sums = tfidf_matrix.sum(axis=0).A1
        top_indices = tfidf_sums.argsort()[-top_k:][::-1]

        return [feature_names[i] for i in top_indices]

    except Exception as e:
        logger.warning("tfidf_extraction_failed", error=str(e))
        return []


def extract_frequency_keywords(
    texts: list[str],
    top_k: int = 10,
    lang: str = "en",
) -> list[str]:
    """Extract top keywords by frequency (fallback method).

    Args:
        texts: List of text documents
        top_k: Number of top keywords to return
        lang: Language for stopword filtering

    Returns:
        List of top keywords
    """
    word_counts: dict[str, int] = defaultdict(int)

    for text in texts:
        tokens = tokenize_simple(text)
        tokens = remove_stopwords(tokens, lang)

        for token in tokens:
            word_counts[token] += 1

    # Sort by frequency
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)

    return [word for word, _ in sorted_words[:top_k]]


def extract_narrative_keywords(
    posts: list[NormalizedPost],
    narratives: list[NarrativeMetadata],
    top_k: int = 10,
    method: str = "tfidf",
) -> list[NarrativeMetadata]:
    """Extract keywords for each narrative.

    Args:
        posts: List of posts with narrative assignments
        narratives: List of narrative metadata
        top_k: Number of keywords per narrative
        method: Extraction method ('tfidf' or 'frequency')

    Returns:
        Updated narrative metadata with keywords
    """
    logger.info("keyword_extraction_started", narrative_count=len(narratives))

    # Group posts by narrative
    narrative_posts: dict[str, list[NormalizedPost]] = defaultdict(list)
    for post in posts:
        if post.narrative_id and post.narrative_id != "noise":
            narrative_posts[post.narrative_id].append(post)

    # Extract keywords for each narrative
    for narrative in narratives:
        posts_in_narrative = narrative_posts.get(narrative.id, [])

        if not posts_in_narrative:
            continue

        texts = [p.text_clean or p.text for p in posts_in_narrative]

        # Determine dominant language
        lang_counts: dict[str, int] = defaultdict(int)
        for p in posts_in_narrative:
            lang_counts[p.lang or "en"] += 1
        dominant_lang = max(lang_counts.items(), key=lambda x: x[1])[0]

        # Extract keywords
        if method == "tfidf":
            keywords = extract_tfidf_keywords(texts, top_k=top_k)
        else:
            keywords = extract_frequency_keywords(texts, top_k=top_k, lang=dominant_lang)

        narrative.keywords = keywords

    logger.info("keyword_extraction_completed", narrative_count=len(narratives))

    return narratives
