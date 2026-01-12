"""Template-based fallback explanation generation."""

from typing import Any

from narrative_graph.ingestion.schemas import (
    CoordinatedGroup,
    Explanation,
    NarrativeMetadata,
    NarrativeRisk,
    RiskLevel,
)
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


class FallbackExplainer:
    """Generate explanations using templates (no LLM required)."""

    def explain_narrative(
        self,
        narrative: NarrativeMetadata,
        risk: NarrativeRisk,
        additional_facts: dict[str, Any] | None = None,
    ) -> Explanation:
        """Generate template-based explanation for a narrative.

        Args:
            narrative: Narrative metadata
            risk: Risk assessment
            additional_facts: Additional facts (unused in template)

        Returns:
            Generated explanation
        """
        # Build explanation text
        paragraphs = []

        # Paragraph 1: What is this narrative
        keywords_str = ", ".join(narrative.keywords[:5]) if narrative.keywords else "various topics"
        hashtags_str = ", ".join(f"#{h}" for h in narrative.top_hashtags[:3]) if narrative.top_hashtags else "no specific hashtags"
        domains_str = ", ".join(narrative.top_domains[:3]) if narrative.top_domains else "various sources"

        para1 = (
            f"This narrative ({narrative.id}) consists of {narrative.size} posts "
            f"discussing topics related to: {keywords_str}. "
            f"The conversation primarily uses hashtags like {hashtags_str} "
            f"and shares content from domains including {domains_str}."
        )
        paragraphs.append(para1)

        # Paragraph 2: Scale and reach
        time_span_str = ""
        if narrative.start_time and narrative.end_time:
            duration = narrative.end_time - narrative.start_time
            hours = duration.total_seconds() / 3600
            if hours < 24:
                time_span_str = f"over {hours:.1f} hours"
            else:
                days = hours / 24
                time_span_str = f"over {days:.1f} days"

        platforms_str = ", ".join(narrative.platforms) if narrative.platforms else "multiple platforms"

        para2 = (
            f"The narrative involves {narrative.author_count} unique authors "
            f"posting across {platforms_str}"
            + (f" {time_span_str}" if time_span_str else "")
            + ". "
        )

        # Add velocity info if high
        if risk.components.velocity > 0.5:
            para2 += "The posting rate indicates rapid spread of this content. "

        paragraphs.append(para2)

        # Paragraph 3: Risk assessment
        risk_intro = {
            RiskLevel.HIGH: "This narrative presents HIGH risk",
            RiskLevel.MEDIUM: "This narrative presents MEDIUM risk",
            RiskLevel.LOW: "This narrative presents LOW risk",
        }

        para3 = f"{risk_intro[risk.risk_level]} (score: {risk.risk_score:.2f}). "

        # Add specific risk factors
        risk_factors = []

        if risk.components.coordination_density > 0.3:
            risk_factors.append("coordinated behavior among accounts")

        if risk.components.bot_score > 0.3:
            risk_factors.append("bot-like activity patterns")

        if risk.components.foreign_domain_ratio > 0.3:
            risk_factors.append("significant foreign domain presence")

        if risk.components.toxicity > 0.3:
            risk_factors.append("potentially toxic content")

        if risk.components.velocity > 0.5:
            risk_factors.append("unusually high posting velocity")

        if risk_factors:
            para3 += f"Key risk factors include: {', '.join(risk_factors)}."
        else:
            para3 += "No significant risk factors were identified."

        paragraphs.append(para3)

        explanation_text = "\n\n".join(paragraphs)

        # Build facts used
        facts = {
            "narrative_id": narrative.id,
            "size": narrative.size,
            "author_count": narrative.author_count,
            "keywords": narrative.keywords[:10],
            "top_domains": narrative.top_domains[:5],
            "top_hashtags": narrative.top_hashtags[:5],
            "risk_score": risk.risk_score,
            "risk_level": risk.risk_level.value,
            "risk_components": risk.components.model_dump(),
        }

        return Explanation(
            target_id=narrative.id,
            target_type="narrative",
            explanation_text=explanation_text,
            facts_used=facts,
            model_info="template-based",
        )

    def explain_coordination(
        self,
        group: CoordinatedGroup,
        additional_facts: dict[str, Any] | None = None,
    ) -> Explanation:
        """Generate template-based explanation for coordination group.

        Args:
            group: Coordinated group
            additional_facts: Additional facts

        Returns:
            Generated explanation
        """
        paragraphs = []

        # Paragraph 1: Overview
        para1 = (
            f"A coordinated group ({group.id}) of {group.size} accounts has been detected "
            f"with a coordination score of {group.score:.2f}. "
            f"These accounts exhibit synchronized behavior patterns that suggest "
            f"potential coordinated influence operations."
        )
        paragraphs.append(para1)

        # Paragraph 2: Accounts involved
        sample_accounts = group.author_ids[:5]
        accounts_str = ", ".join(sample_accounts)
        remaining = group.size - len(sample_accounts)

        para2 = f"Accounts in this group include: {accounts_str}"
        if remaining > 0:
            para2 += f" and {remaining} others"
        para2 += ". "

        if group.narrative_ids:
            narratives_str = ", ".join(group.narrative_ids[:3])
            para2 += f"This group is active in narratives: {narratives_str}."

        paragraphs.append(para2)

        # Paragraph 3: Evidence
        para3 = (
            f"Evidence of coordination: {group.evidence_summary}. "
            f"The high coordination score indicates these accounts likely operate "
            f"in a synchronized manner, potentially as part of an influence campaign."
        )
        paragraphs.append(para3)

        explanation_text = "\n\n".join(paragraphs)

        facts = {
            "group_id": group.id,
            "size": group.size,
            "score": group.score,
            "author_ids": group.author_ids[:10],
            "narrative_ids": group.narrative_ids,
        }

        if additional_facts:
            facts.update(additional_facts)

        return Explanation(
            target_id=group.id,
            target_type="coordination_group",
            explanation_text=explanation_text,
            facts_used=facts,
            model_info="template-based",
        )


def generate_fallback_explanation(
    narrative: NarrativeMetadata,
    risk: NarrativeRisk,
    additional_facts: dict[str, Any] | None = None,
) -> Explanation:
    """Convenience function to generate fallback explanation.

    Args:
        narrative: Narrative metadata
        risk: Risk assessment
        additional_facts: Additional facts

    Returns:
        Generated explanation
    """
    explainer = FallbackExplainer()
    return explainer.explain_narrative(narrative, risk, additional_facts)
