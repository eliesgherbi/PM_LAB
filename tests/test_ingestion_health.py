from __future__ import annotations

from polymarket_insight.data.storage.manifest import Manifest
from polymarket_insight.ingestion.health import detect_missing_interval, snapshot_health


def test_snapshot_health_detects_gap(tmp_path):
    path = tmp_path / "manifest.duckdb"
    detect_missing_interval(path, table_name="orderbook_snapshots", entity_id="123")

    report = snapshot_health(Manifest(path))

    assert report["data_gaps"] == 1
    assert report["recent_gaps"][0]["entity_id"] == "123"
