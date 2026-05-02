"""Daily leaderboard ingestion."""

from __future__ import annotations

from polymarket_insight.data.clients.data_api import LEADERBOARD_CATEGORIES
from polymarket_insight.data.normalize import normalize_raw
from polymarket_insight.ingestion.runner import Runtime


def run_leaderboard_job(runtime: Runtime | None = None) -> dict[str, int]:
    """Snapshot public leaderboard categories and periods."""

    rt = runtime or Runtime()
    for category in sorted(LEADERBOARD_CATEGORIES):
        for period in ("DAY", "WEEK", "MONTH", "ALL"):
            for order_by in ("PNL", "VOL"):
                rt.data_api.get_leaderboard(
                    category=category,
                    time_period=period,
                    order_by=order_by,
                    limit=50,
                    save_raw=True,
                )
    return normalize_raw("data_api", rt.raw_store, rt.normalized_store, rt.manifest)
