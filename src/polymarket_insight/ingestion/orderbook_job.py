"""Orderbook snapshot ingestion."""

from __future__ import annotations

import time

from polymarket_insight.data.datasets.markets import list as list_markets
from polymarket_insight.data.normalize import normalize_raw
from polymarket_insight.ingestion.runner import Runtime
from polymarket_insight.ingestion.universe import select_liquid_markets


def run_orderbook_once(runtime: Runtime | None = None) -> dict[str, int]:
    """Fetch orderbooks for a liquid-market token universe with partial success."""

    rt = runtime or Runtime()
    markets = list_markets(store=rt.normalized_store)
    liquid = select_liquid_markets(
        markets,
        min_24h_volume_usd=rt.settings.ingestion.liquid_market_min_24h_volume_usd,
        resolving_within_days=rt.settings.ingestion.include_markets_resolving_within_days,
    )
    token_ids: list[str] = []
    for _, row in liquid.iterrows():
        ids = row.get("token_ids") or []
        if isinstance(ids, list):
            token_ids.extend(str(token_id) for token_id in ids)
    for token_id in sorted(set(token_ids)):
        try:
            rt.clob.get_book(token_id, save_raw=True)
        except Exception as exc:  # noqa: BLE001 - gap logging keeps batch alive
            rt.manifest.record_gap("orderbook_snapshots", entity_id=token_id, reason=str(exc))
    return normalize_raw("clob", rt.raw_store, rt.normalized_store, rt.manifest)


def run_orderbook_loop(runtime: Runtime | None = None) -> None:
    """Run orderbook ingestion forever at the configured interval."""

    rt = runtime or Runtime()
    while True:
        run_orderbook_once(rt)
        time.sleep(rt.settings.ingestion.orderbook_snapshot_interval_s)
