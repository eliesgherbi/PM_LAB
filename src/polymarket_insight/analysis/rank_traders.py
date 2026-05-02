"""Trader ranking analysis."""

from __future__ import annotations

import pandas as pd

from polymarket_insight.data.datasets import traders as trader_data
from polymarket_insight.metrics.trader import market_count, trade_count


def rank_traders(
    *,
    universe: str,
    category: str = "OVERALL",
    period: str = "MONTH",
    min_trades: int = 20,
    min_markets: int = 5,
    metric: str = "copyability_v0",
) -> pd.DataFrame:
    """Rank traders from local leaderboard and trade data."""

    _ = universe, metric
    leaderboard = trader_data.leaderboard()
    if leaderboard.empty:
        return pd.DataFrame()
    subset = leaderboard[
        (leaderboard["category"].str.upper() == category.upper())
        & (leaderboard["period"].str.upper() == period.upper())
    ].copy()
    rows = []
    for _, row in subset.iterrows():
        wallet = row["wallet"]
        trades = trader_data.trades(wallet)
        n_trades = trade_count(trades)
        n_markets = market_count(trades)
        if n_trades < min_trades or n_markets < min_markets:
            label = "inconclusive"
        else:
            label = "copyable" if float(row.get("pnl") or 0) > 0 else "not_copyable"
        rows.append(
            {
                "wallet": wallet,
                "rank_source": "official_leaderboard",
                "official_leaderboard_pnl": row.get("pnl"),
                "volume": row.get("volume"),
                "trade_count": n_trades,
                "market_count": n_markets,
                "category_exposure": None,
                "profit_concentration_proxy": None,
                "copy_delay_summary": None,
                "copyability_label": label,
                "notes": "Research classification from local normalized data.",
            }
        )
    return pd.DataFrame(rows)
