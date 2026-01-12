"""Text processing utilities."""

import re
import unicodedata

from narrative_graph.logging import get_logger

logger = get_logger(__name__)

# Lazy load langdetect to avoid import overhead
_langdetect = None


def _get_langdetect():
    """Lazy load langdetect module."""
    global _langdetect
    if _langdetect is None:
        from langdetect import detect, DetectorFactory
        # Set seed for reproducibility
        DetectorFactory.seed = 42
        _langdetect = detect
    return _langdetect


def clean_text(text: str, remove_urls: bool = True, remove_mentions: bool = False) -> str:
    """Clean and normalize text content.

    Args:
        text: Raw text content
        remove_urls: Whether to remove URLs
        remove_mentions: Whether to remove @mentions

    Returns:
        Cleaned text
    """
    if not text:
        return ""

    # Normalize unicode
    text = unicodedata.normalize("NFKC", text)

    # Remove URLs
    if remove_urls:
        text = re.sub(r'https?://[^\s<>"{}|\\^`\[\]]+', "", text)

    # Remove mentions
    if remove_mentions:
        text = re.sub(r"@\w+", "", text)

    # Remove RT prefix (retweets)
    text = re.sub(r"^RT\s+", "", text)

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)

    # Strip leading/trailing whitespace
    text = text.strip()

    return text


def detect_language(text: str, fallback: str = "en") -> str:
    """Detect language of text content.

    Args:
        text: Text content
        fallback: Fallback language code if detection fails

    Returns:
        ISO 639-1 language code
    """
    if not text or len(text.strip()) < 10:
        return fallback

    try:
        detect = _get_langdetect()
        # Clean text for better detection
        clean = clean_text(text, remove_urls=True, remove_mentions=True)
        if len(clean) < 10:
            return fallback
        return detect(clean)
    except Exception as e:
        logger.debug("language_detection_failed", error=str(e))
        return fallback


def tokenize_simple(text: str) -> list[str]:
    """Simple word tokenization.

    Args:
        text: Text content

    Returns:
        List of tokens
    """
    # Remove punctuation except apostrophes
    text = re.sub(r"[^\w\s']", " ", text)
    # Split on whitespace
    tokens = text.lower().split()
    # Filter short tokens
    return [t for t in tokens if len(t) > 1]


def remove_stopwords(tokens: list[str], lang: str = "en") -> list[str]:
    """Remove common stopwords from token list.

    Args:
        tokens: List of tokens
        lang: Language code

    Returns:
        Filtered token list
    """
    # Basic English stopwords
    stopwords_en = {
        "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
        "be", "have", "has", "had", "do", "does", "did", "will", "would",
        "could", "should", "may", "might", "must", "shall", "can", "need",
        "it", "its", "this", "that", "these", "those", "i", "you", "he",
        "she", "we", "they", "me", "him", "her", "us", "them", "my", "your",
        "his", "our", "their", "what", "which", "who", "whom", "when",
        "where", "why", "how", "all", "each", "every", "both", "few", "more",
        "most", "other", "some", "such", "no", "nor", "not", "only", "own",
        "same", "so", "than", "too", "very", "just", "also", "now", "here",
        "there", "then", "once", "if", "about", "into", "through", "during",
        "before", "after", "above", "below", "up", "down", "out", "off",
        "over", "under", "again", "further", "any", "because", "until",
        "while", "get", "got", "like", "amp", "rt", "via",
    }

    # Basic Spanish stopwords
    stopwords_es = {
        "el", "la", "los", "las", "un", "una", "unos", "unas", "y", "o",
        "pero", "en", "de", "del", "al", "a", "por", "para", "con", "sin",
        "sobre", "entre", "es", "son", "era", "eran", "fue", "fueron",
        "ser", "estar", "tiene", "tienen", "tenía", "tenían", "que", "se",
        "su", "sus", "este", "esta", "estos", "estas", "ese", "esa", "esos",
        "esas", "yo", "tu", "él", "ella", "nosotros", "ellos", "ellas",
        "mi", "tu", "nuestro", "vuestro", "qué", "cuál", "quién", "cuándo",
        "dónde", "cómo", "más", "menos", "muy", "mucho", "poco", "todo",
        "nada", "algo", "alguien", "nadie", "cada", "otro", "mismo", "ya",
        "también", "solo", "sólo", "así", "aquí", "ahí", "allí", "cuando",
        "donde", "como", "si", "no", "sí", "porque", "aunque", "mientras",
        "hasta", "desde", "hacia", "según", "mediante", "durante", "antes",
        "después", "le", "les", "lo", "me", "te", "nos", "os", "hay",
    }

    stopwords = stopwords_en if lang == "en" else stopwords_es

    return [t for t in tokens if t.lower() not in stopwords]


def extract_ngrams(tokens: list[str], n: int = 2) -> list[str]:
    """Extract n-grams from token list.

    Args:
        tokens: List of tokens
        n: N-gram size

    Returns:
        List of n-grams as joined strings
    """
    if len(tokens) < n:
        return []
    return ["_".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)]
