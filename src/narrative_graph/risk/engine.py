"""Risk scoring engine."""

from collections import defaultdict
from typing import Any

from narrative_graph.config import get_settings
from narrative_graph.graph.connection import Neo4jConnection, get_neo4j_connection
from narrative_graph.graph import queries
from narrative_graph.ingestion.schemas import (
    CoordinatedGroup,
    NarrativeMetadata,
    NarrativeRisk,
    NormalizedPost,
    RiskComponents,
    RiskLevel,
)
from narrative_graph.logging import get_logger
from narrative_graph.risk.components import (
    calculate_bot_score,
    calculate_coordination_score,
    calculate_foreign_domain_score,
    calculate_toxicity_score,
    calculate_velocity_score,
)

logger = get_logger(__name__)


class RiskEngine:
    """Engine for calculating narrative risk scores."""

    def __init__(self, connection: Neo4jConnection | None = None):
        """Initialize risk engine.

        Args:
            connection: Neo4j connection instance
        """
        self.conn = connection or get_neo4j_connection()
        self.settings = get_settings()

    def calculate_risk(
        self,
        posts: list[NormalizedPost],
        narratives: list[NarrativeMetadata],
        groups: list[CoordinatedGroup],
    ) -> list[NarrativeRisk]:
        """Calculate risk scores for all narratives.

        Args:
            posts: List of all posts
            narratives: List of narrative metadata
            groups: List of coordinated groups

        Returns:
            List of narrative risk assessments
        """
        logger.info("risk_calculation_started", narrative_count=len(narratives))

        # Group posts by narrative
        narrative_posts: dict[str, list[NormalizedPost]] = defaultdict(list)
        for post in posts:
            if post.narrative_id and post.narrative_id != "noise":
                narrative_posts[post.narrative_id].append(post)

        risks: list[NarrativeRisk] = []

        for narrative in narratives:
            posts_in_narrative = narrative_posts.get(narrative.id, [])

            if not posts_in_narrative:
                continue

            risk = self._calculate_narrative_risk(
                narrative,
                posts_in_narrative,
                groups,
            )
            risks.append(risk)

            # Update in Neo4j
            self._store_risk(risk)

        # Sort by risk score
        risks.sort(key=lambda r: r.risk_score, reverse=True)

        logger.info(
            "risk_calculation_completed",
            narrative_count=len(risks),
            high_risk_count=sum(1 for r in risks if r.risk_level == RiskLevel.HIGH),
        )

        return risks

    def _calculate_narrative_risk(
        self,
        narrative: NarrativeMetadata,
        posts: list[NormalizedPost],
        groups: list[CoordinatedGroup],
    ) -> NarrativeRisk:
        """Calculate risk for a single narrative.

        Args:
            narrative: Narrative metadata
            posts: Posts in the narrative
            groups: All coordinated groups

        Returns:
            NarrativeRisk assessment
        """
        weights = self.settings.risk.weights
        thresholds = self.settings.risk.thresholds

        # Calculate components
        components = RiskComponents(
            velocity=calculate_velocity_score(posts, narrative),
            coordination_density=calculate_coordination_score(
                narrative.id, groups, narrative.author_count
            ),
            bot_score=calculate_bot_score(posts),
            foreign_domain_ratio=calculate_foreign_domain_score(posts),
            toxicity=calculate_toxicity_score(posts),
        )

        # Calculate weighted score
        risk_score = (
            weights.velocity * components.velocity
            + weights.coordination_density * components.coordination_density
            + weights.bot_score * components.bot_score
            + weights.foreign_domain_ratio * components.foreign_domain_ratio
            + weights.toxicity * components.toxicity
        )

        # Determine risk level
        if risk_score >= thresholds.high:
            risk_level = RiskLevel.HIGH
        elif risk_score >= thresholds.medium:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.LOW

        # Generate reasons
        reasons = self._generate_reasons(components, weights)

        return NarrativeRisk(
            narrative_id=narrative.id,
            risk_score=round(risk_score, 4),
            risk_level=risk_level,
            components=components,
            reasons=reasons,
        )

    def _generate_reasons(
        self,
        components: RiskComponents,
        weights: Any,
    ) -> list[str]:
        """Generate human-readable reasons for risk score.

        Args:
            components: Risk components
            weights: Component weights

        Returns:
            List of reason strings
        """
        reasons = []
        threshold = 0.3  # Minimum component score to report

        if components.velocity >= threshold:
            contribution = weights.velocity * components.velocity
            reasons.append(
                f"High posting velocity ({components.velocity:.2f}) - "
                f"contributes {contribution:.2f} to risk"
            )

        if components.coordination_density >= threshold:
            contribution = weights.coordination_density * components.coordination_density
            reasons.append(
                f"Coordinated behavior detected ({components.coordination_density:.2f}) - "
                f"contributes {contribution:.2f} to risk"
            )

        if components.bot_score >= threshold:
            contribution = weights.bot_score * components.bot_score
            reasons.append(
                f"Bot-like activity patterns ({components.bot_score:.2f}) - "
                f"contributes {contribution:.2f} to risk"
            )

        if components.foreign_domain_ratio >= threshold:
            contribution = weights.foreign_domain_ratio * components.foreign_domain_ratio
            reasons.append(
                f"High foreign domain ratio ({components.foreign_domain_ratio:.2f}) - "
                f"contributes {contribution:.2f} to risk"
            )

        if components.toxicity >= threshold:
            contribution = weights.toxicity * components.toxicity
            reasons.append(
                f"Toxic content indicators ({components.toxicity:.2f}) - "
                f"contributes {contribution:.2f} to risk"
            )

        if not reasons:
            reasons.append("No significant risk factors identified")

        return reasons

    def _store_risk(self, risk: NarrativeRisk) -> None:
        """Store risk assessment in Neo4j.

        Args:
            risk: Risk assessment to store
        """
        self.conn.execute_write(
            queries.UPDATE_NARRATIVE_RISK,
            {
                "narrative_id": risk.narrative_id,
                "risk_score": risk.risk_score,
                "risk_level": risk.risk_level.value,
                "risk_components": risk.components.model_dump_json(),
                "explanation": "\n".join(risk.reasons),
            },
        )


def calculate_narrative_risk(
    posts: list[NormalizedPost],
    narratives: list[NarrativeMetadata],
    groups: list[CoordinatedGroup],
    connection: Neo4jConnection | None = None,
) -> list[NarrativeRisk]:
    """Convenience function to calculate narrative risks.

    Args:
        posts: List of all posts
        narratives: List of narrative metadata
        groups: List of coordinated groups
        connection: Optional Neo4j connection

    Returns:
        List of narrative risk assessments
    """
    engine = RiskEngine(connection=connection)
    return engine.calculate_risk(posts, narratives, groups)
