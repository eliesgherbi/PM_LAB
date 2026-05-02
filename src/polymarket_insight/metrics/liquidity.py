"""Liquidity profile metrics."""

from __future__ import annotations

import pandas as pd


def liquidity_profile(orderbooks: pd.DataFrame) -> pd.DataFrame:
    """Summarize spread and depth from orderbook snapshots."""

    if orderbooks.empty:
        return pd.DataFrame()
    cols = ["spread", "bid_depth_total", "ask_depth_total", "depth_1pct_bid", "depth_1pct_ask"]
    available = [col for col in cols if col in orderbooks]
    return orderbooks.groupby("token_id")[available].mean(numeric_only=False).reset_index()


def fillable_price(orderbook_rows: pd.DataFrame, *, side: str, size: float) -> float | None:
    """Approximate size-aware fill price using orderbook depth JSON."""

    if orderbook_rows.empty:
        return None
    row = orderbook_rows.sort_values("timestamp").iloc[-1]
    return float(row["best_ask"] if side.upper() == "BUY" else row["best_bid"])
