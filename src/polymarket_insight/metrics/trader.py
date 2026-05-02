"""Trader metrics with explicit semantics."""

from __future__ import annotations

from decimal import Decimal

import pandas as pd


def official_leaderboard_pnl(leaderboard_rows: pd.DataFrame) -> Decimal | None:
    """Return official Polymarket leaderboard PnL from normalized leaderboard rows."""

    if leaderboard_rows.empty or "pnl" not in leaderboard_rows:
        return None
    value = leaderboard_rows.sort_values("snapshot_ts").iloc[-1]["pnl"]
    return None if pd.isna(value) else Decimal(str(value))


def trade_count(trades: pd.DataFrame) -> int:
    """Count public trades."""

    return int(len(trades))


def market_count(trades: pd.DataFrame) -> int:
    """Count unique condition IDs traded."""

    return int(trades["condition_id"].nunique()) if "condition_id" in trades else 0


def resolved_market_count(trades: pd.DataFrame, markets: pd.DataFrame) -> int:
    """Count traded markets that are marked resolved in market metadata."""

    if trades.empty or markets.empty:
        return 0
    resolved_ids = set(
        markets.loc[markets.get("resolved", False) == True, "condition_id"]  # noqa: E712
    )
    return len(set(trades["condition_id"]).intersection(resolved_ids))


def volume(trades: pd.DataFrame) -> Decimal:
    """Sum trade notional in USD from normalized trades."""

    if trades.empty or "notional_usd" not in trades:
        return Decimal("0")
    return sum((Decimal(str(v)) for v in trades["notional_usd"].dropna()), Decimal("0"))


def avg_trade_size(trades: pd.DataFrame) -> Decimal | None:
    """Average normalized trade size."""

    if trades.empty or "size" not in trades:
        return None
    return volume_from_column(trades, "size") / Decimal(len(trades))


def median_trade_size(trades: pd.DataFrame) -> Decimal | None:
    """Median normalized trade size."""

    if trades.empty or "size" not in trades:
        return None
    return Decimal(str(pd.to_numeric(trades["size"]).median()))


def category_exposure(trades: pd.DataFrame, markets: pd.DataFrame) -> pd.DataFrame:
    """Estimate category exposure by trade notional."""

    if trades.empty:
        return pd.DataFrame(columns=["category", "notional_usd", "share"])
    if markets.empty:
        merged = trades.copy()
        merged["category"] = None
    else:
        merged = trades.merge(markets[["condition_id", "category"]], on="condition_id", how="left")
    grouped = merged.groupby("category", dropna=False)["notional_usd"].sum().reset_index()
    total = grouped["notional_usd"].sum()
    grouped["share"] = grouped["notional_usd"] / total if total else 0
    return grouped.sort_values("notional_usd", ascending=False).reset_index(drop=True)


def profit_concentration_proxy(leaderboard_rows: pd.DataFrame) -> Decimal | None:
    """Proxy concentration using top official PnL divided by total positive official PnL."""

    if leaderboard_rows.empty or "pnl" not in leaderboard_rows:
        return None
    pnl = pd.to_numeric(leaderboard_rows["pnl"], errors="coerce").fillna(0)
    positives = pnl[pnl > 0]
    if positives.empty or positives.sum() == 0:
        return None
    return Decimal(str(positives.max() / positives.sum()))


def entry_edge_vs_resolution(trades: pd.DataFrame, markets: pd.DataFrame) -> pd.DataFrame:
    """Compute entry edge versus resolved binary outcome, not PnL."""

    if trades.empty or markets.empty:
        return pd.DataFrame()
    resolved = markets[["condition_id", "winning_outcome"]].dropna()
    merged = trades.merge(resolved, on="condition_id", how="inner")
    if merged.empty:
        return merged
    merged["resolution_value"] = (merged["outcome"] == merged["winning_outcome"]).astype(float)
    merged["entry_edge_vs_resolution"] = merged["resolution_value"] - pd.to_numeric(merged["price"])
    return merged


def entry_edge_vs_close(trades: pd.DataFrame, close_prices: pd.DataFrame) -> pd.DataFrame:
    """Compute entry edge versus provided close prices, not PnL."""

    if trades.empty or close_prices.empty:
        return pd.DataFrame()
    merged = trades.merge(close_prices[["token_id", "close_price"]], on="token_id", how="inner")
    merged["entry_edge_vs_close"] = pd.to_numeric(merged["close_price"]) - pd.to_numeric(
        merged["price"]
    )
    return merged


def volume_from_column(df: pd.DataFrame, column: str) -> Decimal:
    """Sum a Decimal-like DataFrame column."""

    return sum((Decimal(str(v)) for v in df[column].dropna()), Decimal("0"))
