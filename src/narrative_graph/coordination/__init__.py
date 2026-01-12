"""Coordination detection module."""

from narrative_graph.coordination.detector import (
    CoordinationDetector,
    detect_coordination,
)
from narrative_graph.coordination.evidence import generate_evidence_summary

__all__ = [
    "CoordinationDetector",
    "detect_coordination",
    "generate_evidence_summary",
]
