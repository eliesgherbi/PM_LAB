"""Coverage and confidence labels for research outputs."""

from __future__ import annotations


def coverage_status(score: float) -> str:
    """Map a 0-1 coverage score to a quality label."""

    if score >= 0.85:
        return "good"
    if score >= 0.60:
        return "partial"
    if score >= 0.30:
        return "weak"
    return "unusable"


def confidence_label(*, sample_size: int, missing_data_ratio: float, coverage: str) -> str:
    """Return a conservative confidence label for a research result."""

    if sample_size < 20 or missing_data_ratio > 0.50 or coverage == "unusable":
        return "insufficient_data"
    if sample_size >= 200 and missing_data_ratio <= 0.10 and coverage == "good":
        return "high_confidence"
    if sample_size >= 75 and missing_data_ratio <= 0.25 and coverage in {"good", "partial"}:
        return "medium_confidence"
    return "low_confidence"
