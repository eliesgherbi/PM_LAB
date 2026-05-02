"""Daily metadata ingestion."""

from __future__ import annotations

from polymarket_insight.data.normalize import normalize_raw
from polymarket_insight.ingestion.runner import Runtime


def run_metadata_job(runtime: Runtime | None = None, *, limit: int = 500) -> dict[str, int]:
    """Fetch market/event metadata, save raw, normalize, and update manifest."""

    rt = runtime or Runtime()
    rt.gamma.list_markets(limit=limit, active="true", save_raw=True)
    rt.gamma.list_events(limit=limit, save_raw=True)
    return normalize_raw("gamma", rt.raw_store, rt.normalized_store, rt.manifest)
