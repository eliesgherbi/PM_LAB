"""Research-run artifact writer."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from polymarket_insight.config import resolve_project_path


def new_run_id(prefix: str) -> str:
    """Create a timestamped research run id."""

    return f"{prefix}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"


def write_research_run(
    *,
    run_id: str,
    input_config: dict[str, Any],
    dataset_coverage: dict[str, Any],
    metrics: pd.DataFrame,
    research_card: str,
    root_dir: str | Path = "research_runs",
) -> Path:
    """Write reproducible research-run artifacts."""

    run_dir = resolve_project_path(root_dir) / run_id
    (run_dir / "figures").mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "created_at": datetime.now(UTC).isoformat(),
                "artifact_version": 1,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    (run_dir / "input_config.json").write_text(
        json.dumps(input_config, indent=2, default=str),
        encoding="utf-8",
    )
    (run_dir / "dataset_coverage.json").write_text(
        json.dumps(dataset_coverage, indent=2, default=str),
        encoding="utf-8",
    )
    metrics.to_parquet(run_dir / "metrics.parquet", index=False)
    (run_dir / "research_card.md").write_text(research_card, encoding="utf-8")
    return run_dir


def write_research_card(path: str | Path, body: str) -> Path:
    """Write a top-level research card."""

    card_path = resolve_project_path(path)
    card_path.parent.mkdir(parents=True, exist_ok=True)
    card_path.write_text(body, encoding="utf-8")
    return card_path
