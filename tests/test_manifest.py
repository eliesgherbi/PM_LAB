from __future__ import annotations

import shutil

import pandas as pd

from polymarket_insight.data.storage.manifest import Manifest


def test_manifest_registers_raw_and_partition(tmp_path, fixture_dir):
    raw = tmp_path / "raw.json"
    shutil.copy(fixture_dir / "trade_raw.json", raw)
    manifest = Manifest(tmp_path / "manifest.duckdb")

    manifest.register_raw_file(raw)
    rows = pd.DataFrame({"timestamp": ["2026-01-01"], "x": [1]})
    manifest.register_partition("trades", tmp_path / "part.parquet", rows, ts_column="timestamp")
    report = manifest.report()

    assert report["raw_files"] == 1
    assert report["normalized_partitions"] == 1
