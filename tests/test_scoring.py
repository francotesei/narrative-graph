"""Tests for risk scoring module."""

from datetime import datetime, timedelta

import pytest

from narrative_graph.ingestion.schemas import (
    CoordinatedGroup,
    NarrativeMetadata,
    NormalizedPost,
    Platform,
    RiskLevel,
)
from narrative_graph.risk.components import (
    calculate_velocity_score,
    calculate_coordination_score,
    calculate_foreign_domain_score,
    calculate_bot_score,
    calculate_toxicity_score,
)


@pytest.fixture
def sample_narrative():
    """Create sample narrative metadata."""
    return NarrativeMetadata(
        id="narrative_001",
        size=50,
        keywords=["policy", "change"],
        top_domains=["example.com"],
        top_hashtags=["PolicyChange"],
        start_time=datetime(2024, 1, 15, 10, 0),
        end_time=datetime(2024, 1, 15, 12, 0),
        author_count=10,
    )


@pytest.fixture
def sample_posts_for_risk():
    """Create sample posts for risk scoring."""
    posts = []
    base_time = datetime(2024, 1, 15, 10, 0)

    for i in range(50):
        posts.append(
            NormalizedPost(
                id=f"post_{i:03d}",
                timestamp=base_time + timedelta(minutes=i * 2),
                platform=Platform.TWITTER,
                author_id=f"user_{i % 10:03d}",
                text=f"Test post {i} about policy change",
                hashtags=["policychange"],
                domains=["example.com"] if i % 3 == 0 else [],
                narrative_id="narrative_001",
            )
        )

    return posts


@pytest.fixture
def sample_coordination_groups():
    """Create sample coordination groups."""
    return [
        CoordinatedGroup(
            id="group_001",
            author_ids=["user_000", "user_001", "user_002"],
            score=0.85,
            evidence_summary="High text similarity",
            narrative_ids=["narrative_001"],
            size=3,
        ),
    ]


class TestVelocityScore:
    """Test velocity score calculation."""

    def test_velocity_score_normal(self, sample_posts_for_risk, sample_narrative):
        """Test velocity score for normal posting rate."""
        score = calculate_velocity_score(sample_posts_for_risk, sample_narrative)

        assert 0 <= score <= 1

    def test_velocity_score_high(self, sample_narrative):
        """Test velocity score for high posting rate."""
        # Create many posts in short time
        base_time = datetime(2024, 1, 15, 10, 0)
        posts = [
            NormalizedPost(
                id=f"post_{i}",
                timestamp=base_time + timedelta(seconds=i * 10),
                platform=Platform.TWITTER,
                author_id=f"user_{i}",
                text=f"Rapid post {i}",
            )
            for i in range(100)
        ]

        score = calculate_velocity_score(posts, sample_narrative)

        # High velocity should give high score
        assert score > 0.5

    def test_velocity_score_empty(self, sample_narrative):
        """Test velocity score with no posts."""
        score = calculate_velocity_score([], sample_narrative)
        assert score == 0.0


class TestCoordinationScore:
    """Test coordination score calculation."""

    def test_coordination_score_with_groups(
        self, sample_coordination_groups, sample_narrative
    ):
        """Test coordination score when groups exist."""
        score = calculate_coordination_score(
            "narrative_001",
            sample_coordination_groups,
            total_authors=10,
        )

        assert 0 <= score <= 1
        assert score > 0  # Should have some coordination

    def test_coordination_score_no_groups(self, sample_narrative):
        """Test coordination score with no groups."""
        score = calculate_coordination_score(
            "narrative_001",
            [],
            total_authors=10,
        )

        assert score == 0.0

    def test_coordination_score_unrelated_groups(self, sample_narrative):
        """Test coordination score with unrelated groups."""
        unrelated_groups = [
            CoordinatedGroup(
                id="group_other",
                author_ids=["other_1", "other_2"],
                score=0.9,
                evidence_summary="Different narrative",
                narrative_ids=["narrative_other"],
                size=2,
            )
        ]

        score = calculate_coordination_score(
            "narrative_001",
            unrelated_groups,
            total_authors=10,
        )

        assert score == 0.0


class TestForeignDomainScore:
    """Test foreign domain score calculation."""

    def test_foreign_domain_score_none(self):
        """Test with no foreign domains."""
        posts = [
            NormalizedPost(
                id="1",
                timestamp=datetime.now(),
                platform=Platform.TWITTER,
                author_id="user",
                text="test",
                domains=["example.com", "news.org"],
            )
        ]

        score = calculate_foreign_domain_score(posts)
        assert score == 0.0

    def test_foreign_domain_score_some(self):
        """Test with some foreign domains."""
        posts = [
            NormalizedPost(
                id="1",
                timestamp=datetime.now(),
                platform=Platform.TWITTER,
                author_id="user",
                text="test",
                domains=["example.com", "news.ru", "info.cn"],
            )
        ]

        score = calculate_foreign_domain_score(posts)
        assert score > 0
        assert score < 1

    def test_foreign_domain_score_all(self):
        """Test with all foreign domains."""
        posts = [
            NormalizedPost(
                id="1",
                timestamp=datetime.now(),
                platform=Platform.TWITTER,
                author_id="user",
                text="test",
                domains=["news.ru", "info.cn"],
            )
        ]

        score = calculate_foreign_domain_score(posts)
        assert score == 1.0


class TestBotScore:
    """Test bot score calculation."""

    def test_bot_score_normal(self, sample_posts_for_risk):
        """Test bot score for normal posting patterns."""
        score = calculate_bot_score(sample_posts_for_risk)

        assert 0 <= score <= 1

    def test_bot_score_repetitive(self):
        """Test bot score for repetitive content."""
        base_time = datetime(2024, 1, 15, 10, 0)
        posts = [
            NormalizedPost(
                id=f"post_{i}",
                timestamp=base_time + timedelta(minutes=i),
                platform=Platform.TWITTER,
                author_id="bot_user",
                text="Same message repeated",  # Repetitive
            )
            for i in range(10)
        ]

        score = calculate_bot_score(posts)

        # Repetitive content should increase bot score
        assert score > 0


class TestToxicityScore:
    """Test toxicity score calculation."""

    def test_toxicity_score_clean(self):
        """Test toxicity score for clean content."""
        posts = [
            NormalizedPost(
                id="1",
                timestamp=datetime.now(),
                platform=Platform.TWITTER,
                author_id="user",
                text="This is a nice and friendly message about technology.",
            )
        ]

        score = calculate_toxicity_score(posts)
        assert score < 0.3

    def test_toxicity_score_toxic(self):
        """Test toxicity score for toxic content."""
        posts = [
            NormalizedPost(
                id="1",
                timestamp=datetime.now(),
                platform=Platform.TWITTER,
                author_id="user",
                text="They are the enemy! This is a dangerous conspiracy and hoax!",
            )
        ]

        score = calculate_toxicity_score(posts)
        assert score > 0

    def test_toxicity_score_empty(self):
        """Test toxicity score with no posts."""
        score = calculate_toxicity_score([])
        assert score == 0.0
