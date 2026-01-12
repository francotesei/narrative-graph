"""Pytest fixtures for Narrative Graph tests."""

import json
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def sample_posts_data():
    """Sample post data for testing."""
    return [
        {
            "id": "test_post_001",
            "timestamp": "2024-01-15T10:30:00Z",
            "platform": "twitter",
            "author_id": "user_001",
            "author_handle": "@test_user_1",
            "text": "Test post about policy change #PolicyChange https://example.com/news",
            "lang": "en",
            "urls": ["https://example.com/news"],
            "hashtags": ["PolicyChange"],
            "mentions": [],
        },
        {
            "id": "test_post_002",
            "timestamp": "2024-01-15T10:35:00Z",
            "platform": "twitter",
            "author_id": "user_002",
            "author_handle": "@test_user_2",
            "text": "Another test post about the same topic #PolicyChange",
            "lang": "en",
            "urls": [],
            "hashtags": ["PolicyChange"],
            "mentions": [],
        },
        {
            "id": "test_post_003",
            "timestamp": "2024-01-15T10:40:00Z",
            "platform": "reddit",
            "author_id": "user_003",
            "author_handle": "reddit_user_3",
            "text": "Different topic about technology and AI #Tech #AI",
            "lang": "en",
            "urls": [],
            "hashtags": ["Tech", "AI"],
            "mentions": [],
        },
    ]


@pytest.fixture
def sample_jsonl_file(sample_posts_data, tmp_path):
    """Create a temporary JSONL file with sample data."""
    file_path = tmp_path / "test_data.jsonl"
    with open(file_path, "w") as f:
        for post in sample_posts_data:
            f.write(json.dumps(post) + "\n")
    return file_path


@pytest.fixture
def sample_csv_file(sample_posts_data, tmp_path):
    """Create a temporary CSV file with sample data."""
    import csv

    file_path = tmp_path / "test_data.csv"

    fieldnames = [
        "id", "timestamp", "platform", "author_id", "author_handle",
        "text", "lang", "urls", "hashtags", "mentions"
    ]

    with open(file_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for post in sample_posts_data:
            row = {
                **post,
                "urls": json.dumps(post.get("urls", [])),
                "hashtags": json.dumps(post.get("hashtags", [])),
                "mentions": json.dumps(post.get("mentions", [])),
            }
            writer.writerow(row)

    return file_path


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary output directory."""
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def mock_config(tmp_path, temp_output_dir):
    """Create a mock configuration."""
    from narrative_graph.config import Settings, PathsConfig

    return Settings(
        paths=PathsConfig(
            data_dir=str(tmp_path / "data"),
            outputs_dir=str(temp_output_dir),
            bronze_dir=str(temp_output_dir / "bronze"),
            silver_dir=str(temp_output_dir / "silver"),
            features_dir=str(temp_output_dir / "features"),
            dead_letter_dir=str(temp_output_dir / "dead_letter"),
        )
    )
