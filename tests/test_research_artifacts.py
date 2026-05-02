from __future__ import annotations

import pandas as pd

from polymarket_insight.research.artifacts import write_research_run


def test_research_run_artifacts(tmp_path):
    run_dir = write_research_run(
        run_id="test_run",
        input_config={"workflow": "test"},
        dataset_coverage={"coverage_status": "good"},
        metrics=pd.DataFrame({"metric": [1]}),
        research_card="# Test\n",
        root_dir=tmp_path,
    )

    assert (run_dir / "manifest.json").exists()
    assert (run_dir / "metrics.parquet").exists()
    assert (run_dir / "research_card.md").read_text(encoding="utf-8") == "# Test\n"
