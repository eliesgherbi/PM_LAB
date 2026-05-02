"""Snapshot health checks."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from polymarket_insight.data.storage.manifest import Manifest


def snapshot_health(manifest: Manifest | None = None) -> dict[str, Any]:
    """Return compact health information and recorded gaps."""

    m = manifest or Manifest()
    report = m.report()
    with m.connect() as con:
        gaps = con.execute(
            """
            SELECT table_name, entity_id, reason, detected_at
            FROM data_gaps
            ORDER BY detected_at DESC
            LIMIT 50
            """
        ).fetchall()
    report["recent_gaps"] = [
        {"table_name": row[0], "entity_id": row[1], "reason": row[2], "detected_at": row[3]}
        for row in gaps
    ]
    report["checked_at"] = datetime.now(UTC)
    return report


def detect_missing_interval(
    manifest_path: str | Path,
    *,
    table_name: str,
    entity_id: str | None,
    reason: str = "missing_interval",
) -> str:
    """Record a missing interval gap for tests and operational checks."""

    return Manifest(manifest_path).record_gap(table_name, entity_id=entity_id, reason=reason)
