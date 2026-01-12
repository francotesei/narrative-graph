"""LLM-based explanation generation."""

import json
from typing import Any

from narrative_graph.config import get_settings
from narrative_graph.ingestion.schemas import (
    CoordinatedGroup,
    Explanation,
    NarrativeMetadata,
    NarrativeRisk,
)
from narrative_graph.logging import get_logger

logger = get_logger(__name__)


NARRATIVE_EXPLANATION_PROMPT = """You are an analyst explaining a detected narrative from social media data.

Based ONLY on the facts provided below, write a clear, factual explanation of this narrative.
Do NOT speculate or add information not present in the facts.
Be concise but thorough.

FACTS:
{facts_json}

Write a 2-3 paragraph explanation covering:
1. What this narrative is about (based on keywords, hashtags, domains)
2. Scale and reach (size, author count, time span)
3. Risk assessment and reasons

Keep the explanation factual and cite specific data points from the facts."""


COORDINATION_EXPLANATION_PROMPT = """You are an analyst explaining detected coordinated behavior.

Based ONLY on the facts provided below, write a clear, factual explanation of this coordination group.
Do NOT speculate or add information not present in the facts.
Be concise but thorough.

FACTS:
{facts_json}

Write a 2-3 paragraph explanation covering:
1. Who is involved (number of accounts, their behavior)
2. Evidence of coordination (shared content, timing, domains)
3. Potential implications

Keep the explanation factual and cite specific data points from the facts."""


class LLMExplainer:
    """Generate explanations using LLM."""

    def __init__(
        self,
        model: str | None = None,
        api_key: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ):
        """Initialize LLM explainer.

        Args:
            model: LLM model name
            api_key: API key
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature
        """
        settings = get_settings()
        self.model = model or settings.llm.model
        self.api_key = api_key or settings.openai_api_key
        self.max_tokens = max_tokens or settings.llm.max_tokens
        self.temperature = temperature or settings.llm.temperature
        self._client = None

    @property
    def client(self):
        """Lazy load OpenAI client."""
        if self._client is None:
            from openai import OpenAI

            self._client = OpenAI(api_key=self.api_key)
            logger.info("openai_client_initialized", model=self.model)
        return self._client

    def is_available(self) -> bool:
        """Check if LLM is available."""
        return bool(self.api_key)

    def explain_narrative(
        self,
        narrative: NarrativeMetadata,
        risk: NarrativeRisk,
        additional_facts: dict[str, Any] | None = None,
    ) -> Explanation:
        """Generate explanation for a narrative.

        Args:
            narrative: Narrative metadata
            risk: Risk assessment
            additional_facts: Additional facts to include

        Returns:
            Generated explanation
        """
        # Build facts dictionary
        facts = {
            "narrative_id": narrative.id,
            "size": narrative.size,
            "author_count": narrative.author_count,
            "keywords": narrative.keywords[:10],
            "top_domains": narrative.top_domains[:5],
            "top_hashtags": narrative.top_hashtags[:5],
            "platforms": narrative.platforms,
            "time_span": {
                "start": narrative.start_time.isoformat() if narrative.start_time else None,
                "end": narrative.end_time.isoformat() if narrative.end_time else None,
            },
            "risk": {
                "score": risk.risk_score,
                "level": risk.risk_level.value,
                "reasons": risk.reasons,
                "components": risk.components.model_dump(),
            },
        }

        if additional_facts:
            facts.update(additional_facts)

        # Generate explanation
        prompt = NARRATIVE_EXPLANATION_PROMPT.format(
            facts_json=json.dumps(facts, indent=2, default=str)
        )

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )

            explanation_text = response.choices[0].message.content or ""

            return Explanation(
                target_id=narrative.id,
                target_type="narrative",
                explanation_text=explanation_text,
                facts_used=facts,
                model_info=f"{self.model}",
            )

        except Exception as e:
            logger.error("llm_explanation_failed", error=str(e))
            raise

    def explain_coordination(
        self,
        group: CoordinatedGroup,
        additional_facts: dict[str, Any] | None = None,
    ) -> Explanation:
        """Generate explanation for a coordination group.

        Args:
            group: Coordinated group
            additional_facts: Additional facts to include

        Returns:
            Generated explanation
        """
        facts = {
            "group_id": group.id,
            "size": group.size,
            "author_ids": group.author_ids[:10],
            "coordination_score": group.score,
            "related_narratives": group.narrative_ids,
            "evidence_summary": group.evidence_summary,
        }

        if additional_facts:
            facts.update(additional_facts)

        prompt = COORDINATION_EXPLANATION_PROMPT.format(
            facts_json=json.dumps(facts, indent=2, default=str)
        )

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )

            explanation_text = response.choices[0].message.content or ""

            return Explanation(
                target_id=group.id,
                target_type="coordination_group",
                explanation_text=explanation_text,
                facts_used=facts,
                model_info=f"{self.model}",
            )

        except Exception as e:
            logger.error("llm_explanation_failed", error=str(e))
            raise


def generate_llm_explanation(
    narrative: NarrativeMetadata,
    risk: NarrativeRisk,
    additional_facts: dict[str, Any] | None = None,
) -> Explanation | None:
    """Convenience function to generate LLM explanation.

    Args:
        narrative: Narrative metadata
        risk: Risk assessment
        additional_facts: Additional facts

    Returns:
        Generated explanation or None if LLM unavailable
    """
    explainer = LLMExplainer()

    if not explainer.is_available():
        logger.warning("llm_not_available", message="No API key configured")
        return None

    return explainer.explain_narrative(narrative, risk, additional_facts)
