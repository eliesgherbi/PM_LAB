"""Market metrics."""

from __future__ import annotations

import pandas as pd


def closing_price_calibration(markets: pd.DataFrame, close_prices: pd.DataFrame) -> pd.DataFrame:
    """Compare close prices to realized binary outcomes."""

    if markets.empty or close_prices.empty:
        return pd.DataFrame()
    resolved = markets[["condition_id", "winning_outcome"]].dropna()
    merged = close_prices.merge(resolved, on="condition_id", how="inner")
    if "outcome" in merged:
        merged["realized"] = (merged["outcome"] == merged["winning_outcome"]).astype(float)
    return merged


def path_volatility(price_points: pd.DataFrame) -> float | None:
    """Return standard deviation of price changes for a token path."""

    if price_points.empty or "price" not in price_points:
        return None
    prices = pd.to_numeric(price_points.sort_values("timestamp")["price"], errors="coerce")
    return float(prices.diff().std())


def trader_concentration(trades: pd.DataFrame) -> pd.DataFrame:
    """Compute trader volume share for a market."""

    if trades.empty:
        return pd.DataFrame(columns=["trader_wallet", "notional_usd", "share"])
    grouped = trades.groupby("trader_wallet")["notional_usd"].sum().reset_index()
    total = grouped["notional_usd"].sum()
    grouped["share"] = grouped["notional_usd"] / total if total else 0
    return grouped.sort_values("share", ascending=False).reset_index(drop=True)
