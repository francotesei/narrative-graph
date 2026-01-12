"""Tests for data ingestion module."""

import json
import pytest

from narrative_graph.ingestion.loaders import load_jsonl, load_csv, load_data, parse_raw_post
from narrative_graph.ingestion.normalizer import (
    normalize_post,
    normalize_platform,
    extract_domain,
    extract_urls_from_text,
    extract_hashtags_from_text,
)
from narrative_graph.ingestion.schemas import RawPost, NormalizedPost, Platform


class TestLoaders:
    """Test data loaders."""

    def test_load_jsonl(self, sample_jsonl_file):
        """Test JSONL loading."""
        records = list(load_jsonl(sample_jsonl_file))

        assert len(records) == 3
        assert all(r[1] is not None for r in records)  # All records loaded
        assert all(r[2] is None for r in records)  # No errors

    def test_load_jsonl_with_errors(self, tmp_path):
        """Test JSONL loading with invalid data."""
        file_path = tmp_path / "invalid.jsonl"
        with open(file_path, "w") as f:
            f.write('{"valid": "json"}\n')
            f.write('invalid json line\n')
            f.write('{"another": "valid"}\n')

        records = list(load_jsonl(file_path))

        assert len(records) == 3
        assert records[0][1] is not None  # Valid
        assert records[1][1] is None  # Invalid
        assert records[1][2] is not None  # Has error
        assert records[2][1] is not None  # Valid

    def test_load_csv(self, sample_csv_file):
        """Test CSV loading."""
        records = list(load_csv(sample_csv_file))

        assert len(records) == 3
        assert all(r[1] is not None for r in records)

    def test_load_data_auto_detect(self, sample_jsonl_file, sample_csv_file):
        """Test auto-detection of file format."""
        jsonl_records = list(load_data(sample_jsonl_file))
        csv_records = list(load_data(sample_csv_file))

        assert len(jsonl_records) == 3
        assert len(csv_records) == 3

    def test_parse_raw_post_valid(self, sample_posts_data):
        """Test parsing valid post data."""
        post, error = parse_raw_post(sample_posts_data[0])

        assert post is not None
        assert error is None
        assert post.id == "test_post_001"
        assert post.platform == "twitter"

    def test_parse_raw_post_invalid(self):
        """Test parsing invalid post data."""
        invalid_data = {"id": "test", "text": "missing required fields"}
        post, error = parse_raw_post(invalid_data)

        assert post is None
        assert error is not None


class TestNormalizer:
    """Test data normalization."""

    def test_normalize_platform(self):
        """Test platform normalization."""
        assert normalize_platform("twitter") == Platform.TWITTER
        assert normalize_platform("Twitter") == Platform.TWITTER
        assert normalize_platform("x") == Platform.TWITTER
        assert normalize_platform("reddit") == Platform.REDDIT
        assert normalize_platform("unknown") == Platform.OTHER

    def test_extract_domain(self):
        """Test domain extraction from URLs."""
        assert extract_domain("https://example.com/path") == "example.com"
        assert extract_domain("https://www.example.com/path") == "example.com"
        assert extract_domain("http://sub.example.com") == "sub.example.com"
        assert extract_domain("invalid") is None

    def test_extract_urls_from_text(self):
        """Test URL extraction from text."""
        text = "Check out https://example.com and http://test.org/page"
        urls = extract_urls_from_text(text)

        assert len(urls) == 2
        assert "https://example.com" in urls
        assert "http://test.org/page" in urls

    def test_extract_hashtags_from_text(self):
        """Test hashtag extraction from text."""
        text = "This is about #Python and #MachineLearning"
        hashtags = extract_hashtags_from_text(text)

        assert len(hashtags) == 2
        assert "Python" in hashtags
        assert "MachineLearning" in hashtags

    def test_normalize_post(self, sample_posts_data):
        """Test post normalization."""
        raw_post = RawPost(**sample_posts_data[0])
        normalized = normalize_post(raw_post)

        assert isinstance(normalized, NormalizedPost)
        assert normalized.id == "test_post_001"
        assert normalized.platform == Platform.TWITTER
        assert "example.com" in normalized.domains
        assert "policychange" in normalized.hashtags
        assert normalized.text_length > 0


class TestSchemas:
    """Test Pydantic schemas."""

    def test_raw_post_timestamp_parsing(self):
        """Test timestamp parsing in RawPost."""
        # ISO format
        post1 = RawPost(
            id="1",
            timestamp="2024-01-15T10:30:00Z",
            platform="twitter",
            author_id="user1",
            text="test",
        )
        assert post1.timestamp is not None

        # Alternative format
        post2 = RawPost(
            id="2",
            timestamp="2024-01-15 10:30:00",
            platform="twitter",
            author_id="user2",
            text="test",
        )
        assert post2.timestamp is not None

    def test_normalized_post_defaults(self):
        """Test NormalizedPost default values."""
        from datetime import datetime

        post = NormalizedPost(
            id="test",
            timestamp=datetime.now(),
            platform=Platform.TWITTER,
            author_id="user",
            text="test text",
        )

        assert post.urls == []
        assert post.domains == []
        assert post.hashtags == []
        assert post.metadata == {}
